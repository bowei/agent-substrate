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

// actionKind describes the transport-agnostic decision returned by the DNS
// protocol handler.
type actionKind string

const (
	actionDrop     actionKind = "Drop"
	actionReply               = "Reply"
	actionForward             = "Forward"
	actionFailover            = "Failover"
	actionDeliver             = "Deliver"
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
	if allow == nil {
		allow = func(dnsmessage.Question) bool { return true }
	}
	return &dnsHandler{
		pending: pending,
		limiter: lim,
		allow:   allow,
	}
}

// requestSanityCheck filters out obviously invalid packets.
func requestSanityCheck(raw []byte) bool {
	// DNS packets need to be at least 12 bytes.
	const dnsHeaderLen = 12
	if len(raw) < dnsHeaderLen {
		return false
	}
	// Query/Response flag must be a query (== 0)
	if raw[2]&0x80 != 0 {
		return false
	}
	return true
}

// errReplyAction returns an Action for an error situation.
func errReplyAction(hdr dnsmessage.Header, rcode dnsmessage.RCode, q *dnsmessage.Question) action {
	return action{
		kind:    actionReply,
		payload: errorPacket(hdr, rcode, q),
	}
}

// typeIXFR is the incremental zone transfer QTYPE (RFC 1995).
const typeIXFR dnsmessage.Type = 251

// onRequest inspects an incoming DNS query packet from the actor and returns
// whether to drop it, reply immediately with a synthesized error, or forward it
// to an upstream resolver.
func (h *dnsHandler) onRequest(raw []byte) action {
	if !requestSanityCheck(raw) {
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
		return errReplyAction(hdr, dnsmessage.RCodeFormatError, nil)
	}

	qdCount := qdCountFromRaw(raw)

	// OpCode must be QUERY (== 0).
	if hdr.OpCode != 0 {
		var qPtr *dnsmessage.Question
		if qdCount == 1 {
			if q, qErr := p.Question(); qErr == nil {
				qPtr = &q
			}
		}
		return errReplyAction(hdr, dnsmessage.RCodeNotImplemented, qPtr)
	}

	// Must have one query.
	if qdCount != 1 {
		return errReplyAction(hdr, dnsmessage.RCodeFormatError, nil)
	}

	// Question is valid.
	q, err := p.Question()
	if err != nil {
		return errReplyAction(hdr, dnsmessage.RCodeFormatError, nil)
	}

	// Question type must be supported.
	if q.Type == dnsmessage.TypeAXFR || q.Type == typeIXFR {
		return errReplyAction(hdr, dnsmessage.RCodeNotImplemented, &q)
	}

	// Check with query policy callout.
	cq := canonicalizeQuestion(q)
	if !h.allow(cq) {
		return errReplyAction(hdr, dnsmessage.RCodeRefused, &q)
	}

	// Set to true for the defer logic.
	forwarded = true

	return action{
		kind:            actionForward,
		clientRequestID: hdr.ID,
		question:        cq,
		hasSlot:         hasSlot,
	}
}

// responseSanityCheck filters out obviously invalid packets.
func responseSanityCheck(raw []byte) bool {
	// DNS packets need to be at least 12 bytes.
	const dnsHeaderLen = 12
	if len(raw) < dnsHeaderLen {
		return false
	}

	// Query/Response flag must be query (!= 0)
	if raw[2]&0x80 == 0 {
		return false
	}

	// Must have exactly one question.
	// Note: this may be too restrictive.
	if qdCountFromRaw(raw) != 1 {
		return false
	}

	return true
}

// onResponse validates an incoming DNS response packet from an upstream
// resolver against pendingRequests and returns whether to drop it, fail over to
// the next upstream, or deliver it to the actor.
func (h *dnsHandler) onResponse(raw []byte, from net.Addr) action {
	if !responseSanityCheck(raw) {
		return action{kind: actionDrop}
	}

	// Parse response.
	var p dnsmessage.Parser
	hdr, err := p.Start(raw)
	if err != nil {
		return action{kind: actionDrop}
	}
	q, err := p.Question()
	if err != nil {
		return action{kind: actionDrop}
	}

	key := pendingKey{
		upstreamID:      hdr.ID,
		transportSource: normalizeAddr(from),
	}

	h.pending.mu.Lock()
	defer h.pending.mu.Unlock()

	// Look for request in the pending table.
	entry, ok := h.pending.entries[key]
	cq := canonicalizeQuestion(q)

	if !ok || entry.question != cq {
		// Response does not match any pending request.
		return action{kind: actionDrop}
	}

	// Check if we need to failover to the next upstream AND there are more
	// upstreams to try.
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

	// Forward response to Actor.
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

// isFailoverRCode returns true if the response error makes sense to try with
// the next DNS upstream server.
func isFailoverRCode(rcode dnsmessage.RCode) bool {
	switch rcode {
	case dnsmessage.RCodeServerFailure, dnsmessage.RCodeNotImplemented, dnsmessage.RCodeRefused:
		return true
	default:
		return false
	}
}

// canonicalizeQuestion normalizes the contents of the Question.
func canonicalizeQuestion(q dnsmessage.Question) dnsmessage.Question {
	for i := range int(q.Name.Length) {
		c := q.Name.Data[i]
		if 'A' <= c && c <= 'Z' {
			q.Name.Data[i] = c + ('a' - 'A')
		}
	}
	clear(q.Name.Data[q.Name.Length:])
	return q
}

// errorPacket returns a serialized DNS packet signaling an error.
func errorPacket(hdr dnsmessage.Header, rcode dnsmessage.RCode, q *dnsmessage.Question) []byte {
	respHdr := dnsmessage.Header{
		ID:                 hdr.ID,
		Response:           true,
		OpCode:             hdr.OpCode,
		RecursionDesired:   hdr.RecursionDesired,
		RecursionAvailable: true,
		RCode:              rcode,
	}

	b := dnsmessage.NewBuilder(nil, respHdr)
	// Try to tack on the question, if possible.
	if q != nil {
		if err := b.StartQuestions(); err == nil {
			_ = b.Question(*q)
		}
	}

	out, err := b.Finish()
	if err == nil {
		return out
	}

	// Last ditch reply: empty DNS response apart from headers that match the
	// request.
	//
	//  0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
	// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	// |                    hdr.ID                     |  bytes 0..1
	// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	// | 1|   OpCode  | 0| 0|RD| 1|  0  0  0|   RCode  |  bytes 2..3
	// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	// |                  QDCOUNT = 0                  |  bytes 4..5
	// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	// |                  ANCOUNT = 0                  |  bytes 6..7
	// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	// |                  NSCOUNT = 0                  |  bytes 8..9
	// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	// |                  ARCOUNT = 0                  |  bytes 10..11
	// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	const dnsHeaderLen = 12
	var fallback [dnsHeaderLen]byte
	binary.BigEndian.PutUint16(fallback[0:2], hdr.ID)
	fallback[2] = 0x80 | (byte(hdr.OpCode)&0x0f)<<3
	if hdr.RecursionDesired {
		fallback[2] |= 0x01
	}
	fallback[3] = 0x80 | (byte(rcode) & 0x0f)

	return fallback[:]
}

// qdCountFromRaw from the raw packet.
//
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
// |                  QDCOUNT = 0                  |  bytes 4..5
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
func qdCountFromRaw(raw []byte) uint16 { return binary.BigEndian.Uint16(raw[4:6]) }
