// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dns

import (
	"bytes"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet/dns/freemap"
	"github.com/agent-substrate/substrate/internal/ateomnet/dns/protocol"
	"golang.org/x/net/dns/dnsmessage"
)

// pendingKey identifies an in-flight request by its rewritten upstream
// transaction ID and the expected upstream source address.
//
// TODO(bowei): inUse guarantees upstreamID is globally unique across in-flight
// requests, so entries can be keyed by upstreamID directly (eliminating map
// re-keying on failover and the linear scan in failOverOnSendError).
type pendingKey struct {
	// upstreamID is the rewritten DNS transaction ID sent to the upstream.
	upstreamID uint16
	// transportSource is the normalized "ip:port" of the active upstream.
	transportSource string
}

// pendingRequest holds the per-query state needed to validate UDP responses,
// restore the actor's transaction ID, and fail over across upstreams.
type pendingRequest struct {
	// clientRequestID is the actor's original DNS transaction ID.
	clientRequestID uint16
	// clientAddr identifies the actor UDP endpoint.
	clientAddr net.Addr
	// question is the canonicalized DNS question used to validate responses (RFC 5452).
	question dnsmessage.Question
	// rawQuery is a copy of the query with upstreamID written to bytes 0..1.
	rawQuery []byte
	// upstreams is the ordered list of upstream "host:port" addresses to try.
	upstreams []string
	// upstreamIdx is the index in upstreams of the current attempt.
	upstreamIdx int
	// deferredResp holds the last failover-eligible error response (e.g. SERVFAIL)
	// to return if all remaining upstreams time out.
	deferredResp []byte
	// expiry is the deadline for the current upstream attempt.
	expiry time.Time
	// hasSlot reports whether this request holds an inFlight limiter slot.
	hasSlot bool
}

// pendingRequests maps (upstreamID, transportSource) to in-flight requests.
//
// TODO(bowei): move the response lookup and failover transition in
// dnsHandler.onUDPResponse into a method on pendingRequests so all lock and state
// transitions live in this file.
type pendingRequests struct {
	// mu guards entries and inUse.
	mu sync.Mutex
	// entries maps active (upstreamID, transportSource) keys to in-flight requests.
	entries map[pendingKey]*pendingRequest
	// inUse tracks allocated 16-bit upstream transaction IDs.
	inUse freemap.Map64k
	// limiter tracks in-flight request slots released when entries are removed.
	limiter *limiter

	// exchangeTimeout bounds the total upstream exchange (defaults to dnsExchangeTimeout).
	exchangeTimeout time.Duration
	// attemptTimeout, if positive, overrides the per-attempt timeout (used in tests).
	attemptTimeout time.Duration
}

// newPendingRequests returns an empty pendingRequests table using lim for
// in-flight slot accounting.
func newPendingRequests(lim *limiter) *pendingRequests {
	return &pendingRequests{
		entries:         make(map[pendingKey]*pendingRequest),
		limiter:         lim,
		exchangeTimeout: dnsExchangeTimeout,
	}
}

// timeoutForAttempt returns the deadline duration for attempt upstreamIdx out
// of numUpstreams. Attempts with fallback upstreams remaining get an equal share
// of exchangeTimeout capped at defaultUpstreamAttemptTimeout; the final upstream
// gets the full exchangeTimeout.
func (p *pendingRequests) timeoutForAttempt(upstreamIdx int, numUpstreams int) time.Duration {
	if p.attemptTimeout > 0 {
		return p.attemptTimeout
	}
	timeout := p.exchangeTimeout
	if timeout <= 0 {
		timeout = dnsExchangeTimeout
	}
	if numUpstreams > 1 && upstreamIdx+1 < numUpstreams {
		return min(timeout/time.Duration(numUpstreams), defaultUpstreamAttemptTimeout)
	}
	return timeout
}

// record allocates a unique upstreamID and stores a pendingRequest entry for
// upstreams[0].It takes ownership of releasing the caller's hasSlot in-flight
// slot on failure or entry removal.
func (p *pendingRequests) record(
	raw []byte,
	clientReqID uint16,
	q dnsmessage.Question,
	clientAddr net.Addr,
	upstreams []string,
	hasSlot bool,
) (uint16, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Probe from a random starting ID so upstream transaction IDs are not predictable.
	start := uint16(rand.Uint32())
	upstreamID, ok := p.inUse.FindFirstUnsetFrom(start)
	if !ok {
		if hasSlot {
			p.limiter.inFlight.release()
		}
		return 0, false
	}

	// Rewrite the packet's transaction ID in place and clone it for failover retries.
	protocol.SetTxnID(raw, upstreamID)
	source := normalizeAddrString(upstreams[0])
	key := pendingKey{
		upstreamID:      upstreamID,
		transportSource: source,
	}
	p.inUse.Set(upstreamID)
	p.entries[key] = &pendingRequest{
		clientRequestID: clientReqID,
		clientAddr:      clientAddr,
		question:        q,
		rawQuery:        bytes.Clone(raw),
		upstreams:       upstreams,
		upstreamIdx:     0,
		expiry:          time.Now().Add(p.timeoutForAttempt(0, len(upstreams))),
		hasSlot:         hasSlot,
	}

	return upstreamID, true
}

// deleteEntryLocked removes key from entries, frees its upstreamID, and
// releases its in-flight slot at most once. p.mu must be held.
func (p *pendingRequests) deleteEntryLocked(key pendingKey, entry *pendingRequest) {
	delete(p.entries, key)
	p.inUse.Clear(key.upstreamID)
	if entry.hasSlot && p.limiter != nil {
		entry.hasSlot = false
		p.limiter.inFlight.release()
	}
}

// failOverOnSendError advances an entry to the next upstream when sending to
// failedIdx fails synchronously, or removes the entry if no upstreams remain.
func (p *pendingRequests) failOverOnSendError(
	upstreamID uint16,
	failedIdx int,
) (nextIdx int, query []byte, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Match both upstreamID and failedIdx so stale errors from earlier attempts are ignored.
	var foundKey pendingKey
	var entry *pendingRequest
	for k, e := range p.entries {
		if k.upstreamID == upstreamID && e.upstreamIdx == failedIdx {
			foundKey = k
			entry = e
			break
		}
	}
	if entry == nil {
		return 0, nil, false
	}
	// Drop the request if no fallback upstreams remain.
	if entry.upstreamIdx+1 >= len(entry.upstreams) {
		p.deleteEntryLocked(foundKey, entry)
		return 0, nil, false
	}

	// Re-key the entry to the next upstream and reset its attempt deadline.
	delete(p.entries, foundKey)
	entry.upstreamIdx++
	entry.expiry = time.Now().Add(p.timeoutForAttempt(entry.upstreamIdx, len(entry.upstreams)))
	nextKey := pendingKey{
		upstreamID:      upstreamID,
		transportSource: normalizeAddrString(entry.upstreams[entry.upstreamIdx]),
	}
	p.entries[nextKey] = entry
	return entry.upstreamIdx, entry.rawQuery, true
}

// clearAll removes all pending entries and releases their in-flight slots.
func (p *pendingRequests) clearAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for key, entry := range p.entries {
		p.deleteEntryLocked(key, entry)
	}
}

// reqFailover describes a timed-out query to resend to the next upstream.
type reqFailover struct {
	upstreamID  uint16
	upstreamIdx int
	query       []byte
}

// reqDelivery describes a deferred response to send back to the actor after
// all upstreams have been exhausted.
type reqDelivery struct {
	clientAddr net.Addr
	payload    []byte
}

// sweep inspects all pending entries at now, advancing expired multi-upstream
// entries to their next upstream and evicting entries that have exhausted all
// upstreams.
func (p *pendingRequests) sweep(now time.Time) ([]reqFailover, []reqDelivery) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var failovers []reqFailover
	var deliveries []reqDelivery

	for key, entry := range p.entries {
		if entry.expiry.IsZero() || !now.After(entry.expiry) {
			continue
		}
		// Advance to the next upstream if fallbacks remain.
		// TODO(bowei): avoid inserting nextKey into p.entries while ranging over it
		// (resolved once entries is keyed by upstreamID).
		if entry.upstreamIdx+1 < len(entry.upstreams) {
			delete(p.entries, key)
			entry.upstreamIdx++
			entry.expiry = now.Add(p.timeoutForAttempt(entry.upstreamIdx, len(entry.upstreams)))
			nextKey := pendingKey{
				upstreamID:      key.upstreamID,
				transportSource: normalizeAddrString(entry.upstreams[entry.upstreamIdx]),
			}
			p.entries[nextKey] = entry
			failovers = append(failovers, reqFailover{
				upstreamID:  key.upstreamID,
				upstreamIdx: entry.upstreamIdx,
				query:       entry.rawQuery,
			})
			continue
		}

		// All upstreams exhausted: evict the entry and deliver any held-back
		// error response (e.g. SERVFAIL from an earlier upstream) with the
		// actor's original transaction ID restored.
		p.deleteEntryLocked(key, entry)
		if entry.deferredResp != nil && entry.clientAddr != nil {
			resp := bytes.Clone(entry.deferredResp)
			protocol.SetTxnID(resp, entry.clientRequestID)
			deliveries = append(deliveries, reqDelivery{
				clientAddr: entry.clientAddr,
				payload:    resp,
			})
		}
	}
	return failovers, deliveries
}
