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
	"log/slog"
	"net"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

const (
	dnsHeaderLen = 12

	// typeIXFR is the incremental zone transfer QTYPE (RFC 1995).
	typeIXFR dnsmessage.Type = 251
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

// queryPolicy decides whether a parsed DNS question is permitted.
type queryPolicy func(q dnsmessage.Question) bool

// dnsHandler inspects DNS requests and responses and returns transport-agnostic
// routing decisions.
type dnsHandler struct {
	pending *pendingRequests
	limiter *limiter
	allow   queryPolicy
}

func newDNSHandler(pending *pendingRequests, lim *limiter, allow queryPolicy) *dnsHandler {
	return &dnsHandler{
		pending: pending,
		limiter: lim,
		allow:   allow,
	}
}

// onRequest inspects an incoming DNS query packet from the actor and returns
// whether to drop it, reply immediately with a synthesized error, or forward it
// to an upstream resolver.
func (h *dnsHandler) onRequest(raw []byte) action {
	if len(raw) < dnsHeaderLen || raw[2]&0x80 != 0 {
		return action{kind: actionDrop}
	}

	hasSlot := false
	forwarded := false

	defer func() {
		if !forwarded && hasSlot {
			h.limiter.inFlight.release()
		}
	}()

	if h.limiter.inFlight.tryAcquire() {
		hasSlot = true
	} else {
		slog.Debug("dns relay dropped a DNS query; too many in flight")
		return action{kind: actionDrop}
	}

	var p dnsmessage.Parser
	hdr, err := p.Start(raw)
	if err != nil {
		return action{
			kind:    actionReply,
			payload: makeReply(hdr, dnsmessage.RCodeFormatError, nil),
		}
	}

	qdCount := binary.BigEndian.Uint16(raw[4:6])
	if hdr.OpCode != 0 {
		var qPtr *dnsmessage.Question
		if qdCount == 1 {
			if q, qErr := p.Question(); qErr == nil {
				qPtr = &q
			}
		}
		return action{
			kind:    actionReply,
			payload: makeReply(hdr, dnsmessage.RCodeNotImplemented, qPtr),
		}
	}

	if qdCount != 1 {
		return action{
			kind:    actionReply,
			payload: makeReply(hdr, dnsmessage.RCodeFormatError, nil),
		}
	}

	q, err := p.Question()
	if err != nil {
		return action{
			kind:    actionReply,
			payload: makeReply(hdr, dnsmessage.RCodeFormatError, nil),
		}
	}

	if q.Type == dnsmessage.TypeAXFR || q.Type == typeIXFR {
		return action{
			kind:    actionReply,
			payload: makeReply(hdr, dnsmessage.RCodeNotImplemented, &q),
		}
	}

	cq := canonicalQuestion(q)
	if h.allow != nil && !h.allow(cq) {
		return action{
			kind:    actionReply,
			payload: makeReply(hdr, dnsmessage.RCodeRefused, &q),
		}
	}

	// Set to true for the flight limiter update on defer.
	forwarded = true

	return action{
		kind:            actionForward,
		clientRequestID: hdr.ID,
		question:        cq,
		hasSlot:         hasSlot,
	}
}

// onResponse validates an incoming DNS response packet from an upstream
// resolver against pendingRequests and returns whether to drop it, fail over to
// the next upstream, or deliver it to the actor.
func (h *dnsHandler) onResponse(raw []byte, from net.Addr) action {
	if len(raw) < dnsHeaderLen || raw[2]&0x80 == 0 {
		return action{kind: actionDrop}
	}
	if binary.BigEndian.Uint16(raw[4:6]) != 1 {
		return action{kind: actionDrop}
	}

	var p dnsmessage.Parser
	hdr, err := p.Start(raw)
	if err != nil {
		return action{kind: actionDrop}
	}
	q, err := p.Question()
	if err != nil {
		return action{kind: actionDrop}
	}
	cq := canonicalQuestion(q)

	key := pendingKey{
		upstreamID:      hdr.ID,
		transportSource: normalizeAddr(from),
	}

	h.pending.mu.Lock()
	defer h.pending.mu.Unlock()

	entry, ok := h.pending.entries[key]
	if !ok || entry.question != cq {
		return action{kind: actionDrop}
	}

	if isFailoverRCode(hdr.RCode) && entry.upstreamIdx+1 < len(entry.upstreams) {
		entry.deferredResp = bytes.Clone(raw)
		delete(h.pending.entries, key)
		entry.upstreamIdx++
		entry.expiry = time.Now().Add(h.pending.timeoutForAttempt(entry.upstreamIdx, len(entry.upstreams)))
		nextKey := pendingKey{
			upstreamID:      hdr.ID,
			transportSource: normalizeAddrString(entry.upstreams[entry.upstreamIdx]),
		}
		h.pending.entries[nextKey] = entry
		return action{
			kind:        actionFailover,
			payload:     bytes.Clone(entry.rawQuery),
			upstreamIdx: entry.upstreamIdx,
		}
	}

	clientID := entry.clientRequestID
	clientSource := entry.clientSource
	h.pending.deleteEntryLocked(key, entry)

	out := bytes.Clone(raw)
	binary.BigEndian.PutUint16(out[0:2], clientID)
	return action{
		kind:         actionDeliver,
		payload:      out,
		clientSource: clientSource,
	}
}

func isFailoverRCode(rcode dnsmessage.RCode) bool {
	switch rcode {
	case dnsmessage.RCodeServerFailure, dnsmessage.RCodeNotImplemented, dnsmessage.RCodeRefused:
		return true
	default:
		return false
	}
}

func canonicalQuestion(q dnsmessage.Question) dnsmessage.Question {
	for i := range int(q.Name.Length) {
		c := q.Name.Data[i]
		if 'A' <= c && c <= 'Z' {
			q.Name.Data[i] = c + ('a' - 'A')
		}
	}
	clear(q.Name.Data[q.Name.Length:])
	return q
}

func makeReply(hdr dnsmessage.Header, rcode dnsmessage.RCode, q *dnsmessage.Question) []byte {
	respHdr := dnsmessage.Header{
		ID:                 hdr.ID,
		Response:           true,
		OpCode:             hdr.OpCode,
		RecursionDesired:   hdr.RecursionDesired,
		RecursionAvailable: true,
		RCode:              rcode,
	}

	b := dnsmessage.NewBuilder(nil, respHdr)
	if q != nil {
		if err := b.StartQuestions(); err == nil {
			_ = b.Question(*q)
		}
	}

	out, err := b.Finish()
	if err != nil {
		var fallback [dnsHeaderLen]byte
		binary.BigEndian.PutUint16(fallback[0:2], hdr.ID)
		fallback[2] = 0x80 | (byte(hdr.OpCode)&0x0f)<<3
		if hdr.RecursionDesired {
			fallback[2] |= 0x01
		}
		fallback[3] = 0x80 | (byte(rcode) & 0x0f)
		return fallback[:]
	}

	return out
}
