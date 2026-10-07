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
	"encoding/binary"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// pendingKey identifies an in-flight request by its rewritten upstream
// transaction ID and the expected upstream source address.
type pendingKey struct {
	upstreamID      uint16
	transportSource string
}

// pendingRequest holds the per-query state needed to validate responses,
// restore the actor's transaction ID, and fail over across upstreams.
type pendingRequest struct {
	clientRequestID uint16
	clientSource    any
	question        dnsmessage.Question
	rawQuery        []byte
	upstreams       []string
	upstreamIdx     int
	deferredResp    []byte
	expiry          time.Time
	hasSlot         bool
}

// pendingRequests maps (upstreamID, transportSource) to in-flight requests.
type pendingRequests struct {
	mu      sync.Mutex
	entries map[pendingKey]*pendingRequest
	inUse   map[uint16]struct{}
	limiter *limiter

	exchangeTimeout time.Duration
	attemptTimeout  time.Duration
}

func newPendingRequests(lim *limiter) *pendingRequests {
	return &pendingRequests{
		entries:         make(map[pendingKey]*pendingRequest),
		inUse:           make(map[uint16]struct{}),
		limiter:         lim,
		exchangeTimeout: dnsExchangeTimeout,
	}
}

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

// record allocates a unique upstreamID, rewrites raw[0:2] to upstreamID, and
// stores a pendingRequest entry for upstreams[0].
func (p *pendingRequests) record(
	raw []byte,
	clientReqID uint16,
	q dnsmessage.Question,
	clientSource any,
	upstreams []string,
	hasSlot bool,
) (uint16, bool) {
	if len(raw) < 2 || len(upstreams) == 0 {
		if hasSlot {
			p.limiter.inFlight.release()
		}
		return 0, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.inUse) >= 65536 {
		if hasSlot {
			p.limiter.inFlight.release()
		}
		return 0, false
	}

	start := uint16(rand.Uint32())
	var upstreamID uint16
	for i := range 65536 {
		candidate := start + uint16(i)
		if _, taken := p.inUse[candidate]; !taken {
			upstreamID = candidate
			break
		}
	}

	binary.BigEndian.PutUint16(raw[0:2], upstreamID)
	source := normalizeAddrString(upstreams[0])
	key := pendingKey{
		upstreamID:      upstreamID,
		transportSource: source,
	}
	p.inUse[upstreamID] = struct{}{}
	p.entries[key] = &pendingRequest{
		clientRequestID: clientReqID,
		clientSource:    clientSource,
		question:        q,
		rawQuery:        bytes.Clone(raw),
		upstreams:       upstreams,
		upstreamIdx:     0,
		expiry:          time.Now().Add(p.timeoutForAttempt(0, len(upstreams))),
		hasSlot:         hasSlot,
	}
	return upstreamID, true
}

func (p *pendingRequests) deleteEntryLocked(key pendingKey, entry *pendingRequest) {
	delete(p.entries, key)
	delete(p.inUse, key.upstreamID)
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
	if entry.upstreamIdx+1 >= len(entry.upstreams) {
		p.deleteEntryLocked(foundKey, entry)
		return 0, nil, false
	}

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

// removeByClientSource removes all pending requests associated with clientSource.
func (p *pendingRequests) removeByClientSource(clientSource any) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for key, entry := range p.entries {
		if entry.clientSource == clientSource {
			p.deleteEntryLocked(key, entry)
		}
	}
}

// countByClientSource returns the number of pending requests for clientSource.
func (p *pendingRequests) countByClientSource(clientSource any) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	n := 0
	for _, entry := range p.entries {
		if entry.clientSource == clientSource {
			n++
		}
	}
	return n
}

// clearAll removes all pending entries and releases their in-flight slots.
func (p *pendingRequests) clearAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for key, entry := range p.entries {
		p.deleteEntryLocked(key, entry)
	}
}

type reqFailover struct {
	upstreamID  uint16
	upstreamIdx int
	query       []byte
}

type reqDelivery struct {
	clientAddr net.Addr
	payload    []byte
}

// sweep inspects all pending entries at `now“, advancing expired multi-upstream
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

		p.deleteEntryLocked(key, entry)
		if entry.deferredResp != nil {
			if addr, ok := entry.clientSource.(net.Addr); ok {
				resp := bytes.Clone(entry.deferredResp)
				binary.BigEndian.PutUint16(resp[0:2], entry.clientRequestID)
				deliveries = append(deliveries, reqDelivery{
					clientAddr: addr,
					payload:    resp,
				})
			}
		}
	}
	return failovers, deliveries
}
