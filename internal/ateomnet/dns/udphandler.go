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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

const (
	// defaultSweepInterval is how often pendingRequests is checked for expired
	// UDP queries.
	defaultSweepInterval = 50 * time.Millisecond

	// defaultMultiUpstreamAttemptTimeout bounds a single UDP upstream attempt
	// when fallback upstreams remain, so failover completes within the client's
	// overall exchange deadline.
	defaultMultiUpstreamAttemptTimeout = time.Second
)

// actionKind describes the transport-agnostic decision returned by the DNS
// protocol handler.
type actionKind uint8

const (
	actionDrop actionKind = iota
	actionReply
	actionForward
	actionFailover
	actionDeliver
)

// action is the decision returned by onRequest and onResponse.
type action struct {
	kind            actionKind
	payload         []byte
	clientRequestID uint16
	question        dnsmessage.Question
	clientSource    any
	upstreamIdx     int
	hasSlot         bool
}

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
	mu              sync.Mutex
	entries         map[pendingKey]*pendingRequest
	inUse           map[uint16]struct{}
	limiter         *limiter
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
		return min(timeout/time.Duration(numUpstreams), defaultMultiUpstreamAttemptTimeout)
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
		if hasSlot && p.limiter != nil {
			<-p.limiter.inFlight
		}
		return 0, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.inUse) >= 65536 {
		if hasSlot && p.limiter != nil {
			<-p.limiter.inFlight
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
		<-p.limiter.inFlight
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

type udpFailover struct {
	upstreamID  uint16
	upstreamIdx int
	query       []byte
}

type udpDelivery struct {
	clientAddr net.Addr
	payload    []byte
}

// sweep inspects all pending entries at now, advancing expired multi-upstream
// entries to their next upstream and evicting entries that have exhausted all
// upstreams.
func (p *pendingRequests) sweep(now time.Time) ([]udpFailover, []udpDelivery) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var failovers []udpFailover
	var deliveries []udpDelivery
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
			failovers = append(failovers, udpFailover{
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
				deliveries = append(deliveries, udpDelivery{
					clientAddr: addr,
					payload:    resp,
				})
			}
		}
	}
	return failovers, deliveries
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

func normalizeAddr(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	switch a := addr.(type) {
	case *net.UDPAddr:
		ap := a.AddrPort()
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()).String()
	case *net.TCPAddr:
		ap := a.AddrPort()
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()).String()
	default:
		return normalizeAddrString(addr.String())
	}
}

func normalizeAddrString(s string) string {
	if ap, err := netip.ParseAddrPort(s); err == nil {
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()).String()
	}
	return s
}

// udpHandler manages actor UDP DNS traffic using an ingress socket in the
// sandbox gateway namespace and an unconnected egress socket in the worker
// namespace.
type udpHandler struct {
	ingress       net.PacketConn
	egress        net.PacketConn
	upstreams     []*net.UDPAddr
	upstreamStrs  []string
	dns           *dnsHandler
	pending       *pendingRequests
	sweepInterval time.Duration
}

func newUDPHandler(
	ingress net.PacketConn,
	egress net.PacketConn,
	upstreams []*net.UDPAddr,
	dns *dnsHandler,
	pending *pendingRequests,
) *udpHandler {
	strs := make([]string, len(upstreams))
	for i, u := range upstreams {
		strs[i] = normalizeAddr(u)
	}
	return &udpHandler{
		ingress:       ingress,
		egress:        egress,
		upstreams:     upstreams,
		upstreamStrs:  strs,
		dns:           dns,
		pending:       pending,
		sweepInterval: defaultSweepInterval,
	}
}

// serve runs the ingress reader, egress reader, and timeout sweep ticker until
// ctx is canceled or the sockets close.
func (h *udpHandler) serve(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	wg.Add(3)
	go func() {
		defer wg.Done()
		if err := h.readIngress(ctx); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		if err := h.readEgress(ctx); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		h.sweepLoop(ctx)
	}()

	wg.Wait()
	h.pending.clearAll()
	close(errCh)

	var errs error
	for err := range errCh {
		errs = errors.Join(errs, err)
	}
	return errs
}

func (h *udpHandler) readIngress(ctx context.Context) error {
	buf := make([]byte, maxDNSDatagram)
	for {
		n, from, err := h.ingress.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("dns: reading actor DNS query: %w", err)
		}
		raw := bytes.Clone(buf[:n])
		act := h.dns.onRequest(raw)
		switch act.kind {
		case actionDrop:
			continue
		case actionReply:
			if _, err := h.ingress.WriteTo(act.payload, from); err != nil && ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
				slog.WarnContext(ctx, "dns relay could not return a synthesized DNS reply", slog.Any("err", err))
			}
		case actionForward:
			upstreamID, ok := h.pending.record(raw, act.clientRequestID, act.question, from, h.upstreamStrs, act.hasSlot)
			if !ok {
				continue
			}
			h.sendToUpstream(ctx, upstreamID, 0, raw)
		}
	}
}

func (h *udpHandler) sendToUpstream(ctx context.Context, upstreamID uint16, idx int, query []byte) {
	for idx < len(h.upstreams) {
		_, err := h.egress.WriteTo(query, h.upstreams[idx])
		if err == nil {
			return
		}
		if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
			h.pending.clearAll()
			return
		}
		slog.WarnContext(ctx, "dns relay could not send query to upstream", slog.String("upstream", h.upstreamStrs[idx]), slog.Any("err", err))
		nextIdx, nextQuery, ok := h.pending.failOverOnSendError(upstreamID, idx)
		if !ok {
			return
		}
		idx = nextIdx
		query = nextQuery
	}
}

func (h *udpHandler) readEgress(ctx context.Context) error {
	buf := make([]byte, maxDNSDatagram)
	for {
		n, from, err := h.egress.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			slog.DebugContext(ctx, "dns relay egress socket read error", slog.Any("err", err))
			continue
		}
		raw := bytes.Clone(buf[:n])
		act := h.dns.onResponse(raw, from)
		switch act.kind {
		case actionDrop:
			continue
		case actionFailover:
			if act.upstreamIdx >= 0 && act.upstreamIdx < len(h.upstreams) && len(act.payload) >= 2 {
				upstreamID := binary.BigEndian.Uint16(act.payload[0:2])
				h.sendToUpstream(ctx, upstreamID, act.upstreamIdx, act.payload)
			}
		case actionDeliver:
			if clientAddr, ok := act.clientSource.(net.Addr); ok {
				if _, err := h.ingress.WriteTo(act.payload, clientAddr); err != nil && ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
					slog.WarnContext(ctx, "dns relay could not return a DNS answer", slog.Any("err", err))
				}
			}
		}
	}
}

func (h *udpHandler) sweepLoop(ctx context.Context) {
	interval := h.sweepInterval
	if interval <= 0 {
		interval = defaultSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			failovers, deliveries := h.pending.sweep(now)
			for _, f := range failovers {
				h.sendToUpstream(ctx, f.upstreamID, f.upstreamIdx, f.query)
			}
			for _, d := range deliveries {
				if _, err := h.ingress.WriteTo(d.payload, d.clientAddr); err != nil && ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
					slog.WarnContext(ctx, "dns relay could not return a deferred DNS answer", slog.Any("err", err))
				}
			}
		}
	}
}
