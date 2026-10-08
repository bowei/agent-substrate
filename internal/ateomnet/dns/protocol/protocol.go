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

package protocol

import (
	"encoding/binary"

	"golang.org/x/net/dns/dnsmessage"
)

// TypeIXFR is the incremental zone transfer QTYPE (RFC 1995).
const TypeIXFR dnsmessage.Type = 251

// RequestSanityCheck filters out obviously invalid packets.
func RequestSanityCheck(raw []byte) bool {
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

// ResponseSanityCheck filters out obviously invalid packets.
func ResponseSanityCheck(raw []byte) bool {
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
	if QDCount(raw) != 1 {
		return false
	}

	return true
}

// SetTxnID in the DNS packet.
func SetTxnID(raw []byte, id uint16) {
	binary.BigEndian.PutUint16(raw[0:2], id)
}

// qdCountFromRaw from the raw packet.
//
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
// |                  QDCOUNT = 0                  |  bytes 4..5
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
func QDCount(raw []byte) uint16 {
	return binary.BigEndian.Uint16(raw[4:6])
}

// ErrorPacket returns a serialized DNS packet signaling an error.
func ErrorPacket(hdr dnsmessage.Header, rcode dnsmessage.RCode, q *dnsmessage.Question) []byte {
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

	// Last ditch reply: somehow we got an error on generating the response.
	// Synthesize an empty DNS response that matches the request.
	const dnsHeaderLen = 12
	fallback := make([]byte, dnsHeaderLen)
	SetTxnID(fallback, hdr.ID)
	fallback[2] = 0x80 | (byte(hdr.OpCode)&0x0f)<<3
	if hdr.RecursionDesired {
		fallback[2] |= 0x01
	}
	fallback[3] = 0x80 | (byte(rcode) & 0x0f)

	return fallback
}
