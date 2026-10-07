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
	"net"
	"testing"

	"golang.org/x/net/dns/dnsmessage"
)

func buildTestMessage(t *testing.T, hdr dnsmessage.Header, questions []dnsmessage.Question) []byte {
	t.Helper()
	b := dnsmessage.NewBuilder(nil, hdr)
	if len(questions) > 0 {
		if err := b.StartQuestions(); err != nil {
			t.Fatal(err)
		}
		for _, q := range questions {
			if err := b.Question(q); err != nil {
				t.Fatal(err)
			}
		}
	}
	msg, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

func TestDNSHandlerOnRequestDrop(t *testing.T) {
	lim := &limiter{
		inFlight:    make(chan struct{}, 1),
		connections: make(chan struct{}, 1),
	}
	pending := newPendingRequests(lim)
	h := newDNSHandler(pending, lim, nil)

	// 1. Short packet (< 12 bytes).
	if act := h.onRequest([]byte{1, 2, 3, 4}); act.kind != actionDrop {
		t.Errorf("short packet action = %v, want actionDrop", act.kind)
	}

	// 2. Packet with QR == 1 (response bit set).
	resp := dnsAnswer(dnsQuery(0x1234), 0)
	if act := h.onRequest(resp); act.kind != actionDrop {
		t.Errorf("QR=1 packet action = %v, want actionDrop", act.kind)
	}

	// 3. Rate-limited (inFlight full).
	lim.inFlight <- struct{}{}
	defer func() { <-lim.inFlight }()
	if act := h.onRequest(dnsQuery(0x1234)); act.kind != actionDrop {
		t.Errorf("rate-limited packet action = %v, want actionDrop", act.kind)
	}
}

func TestDNSHandlerOnRequestSynthesizedReplies(t *testing.T) {
	lim := &limiter{
		inFlight:    make(chan struct{}, maxInFlight),
		connections: make(chan struct{}, maxConnections),
	}
	pending := newPendingRequests(lim)
	allow := func(q dnsmessage.Question) bool {
		return q.Name.String() != "blocked.example.com." && q.Type != dnsmessage.TypeTXT
	}
	h := newDNSHandler(pending, lim, allow)

	qExample := dnsmessage.Question{
		Name:  dnsmessage.MustNewName("example.com."),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}

	tests := []struct {
		name      string
		raw       []byte
		wantRCode dnsmessage.RCode
	}{
		{
			name: "QDCOUNT == 0 -> FORMERR",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1001,
				RecursionDesired: true,
			}, nil),
			wantRCode: dnsmessage.RCodeFormatError,
		},
		{
			name: "QDCOUNT == 2 -> FORMERR",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1002,
				RecursionDesired: true,
			}, []dnsmessage.Question{qExample, qExample}),
			wantRCode: dnsmessage.RCodeFormatError,
		},
		{
			name: "malformed question name -> FORMERR",
			raw: []byte{
				0x10, 0x03, 0x01, 0x00,
				0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x10, 'a', 'b', // label length 16 but truncated
			},
			wantRCode: dnsmessage.RCodeFormatError,
		},
		{
			name: "non-zero OpCode -> NOTIMP",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1004,
				OpCode:           1, // IQUERY
				RecursionDesired: true,
			}, []dnsmessage.Question{qExample}),
			wantRCode: dnsmessage.RCodeNotImplemented,
		},
		{
			name: "QTYPE AXFR -> NOTIMP",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1005,
				RecursionDesired: true,
			}, []dnsmessage.Question{{
				Name:  dnsmessage.MustNewName("example.com."),
				Type:  dnsmessage.TypeAXFR,
				Class: dnsmessage.ClassINET,
			}}),
			wantRCode: dnsmessage.RCodeNotImplemented,
		},
		{
			name: "QTYPE IXFR -> NOTIMP",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1006,
				RecursionDesired: true,
			}, []dnsmessage.Question{{
				Name:  dnsmessage.MustNewName("example.com."),
				Type:  typeIXFR,
				Class: dnsmessage.ClassINET,
			}}),
			wantRCode: dnsmessage.RCodeNotImplemented,
		},
		{
			name: "disallowed domain -> REFUSED",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1007,
				RecursionDesired: true,
			}, []dnsmessage.Question{{
				Name:  dnsmessage.MustNewName("Blocked.Example.COM."),
				Type:  dnsmessage.TypeA,
				Class: dnsmessage.ClassINET,
			}}),
			wantRCode: dnsmessage.RCodeRefused,
		},
		{
			name: "disallowed record type -> REFUSED",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1008,
				RecursionDesired: true,
			}, []dnsmessage.Question{{
				Name:  dnsmessage.MustNewName("example.com."),
				Type:  dnsmessage.TypeTXT,
				Class: dnsmessage.ClassINET,
			}}),
			wantRCode: dnsmessage.RCodeRefused,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantID := binary.BigEndian.Uint16(tc.raw[0:2])
			act := h.onRequest(tc.raw)
			if act.kind != actionReply {
				t.Fatalf("action.kind = %v, want actionReply", act.kind)
			}
			var p dnsmessage.Parser
			hdr, err := p.Start(act.payload)
			if err != nil {
				t.Fatalf("parsing synthesized reply: %v", err)
			}
			if hdr.ID != wantID {
				t.Errorf("reply ID = %#x, want %#x", hdr.ID, wantID)
			}
			if !hdr.Response {
				t.Error("reply QR = false, want true")
			}
			if hdr.RCode != tc.wantRCode {
				t.Errorf("reply RCode = %v, want %v", hdr.RCode, tc.wantRCode)
			}
			if len(lim.inFlight) != 0 {
				t.Errorf("inFlight slots = %d after synthesized reply, want 0", len(lim.inFlight))
			}
		})
	}
}

func TestDNSHandlerForwardAndResponseValidation(t *testing.T) {
	lim := &limiter{
		inFlight:    make(chan struct{}, maxInFlight),
		connections: make(chan struct{}, maxConnections),
	}
	pending := newPendingRequests(lim)
	h := newDNSHandler(pending, lim, nil)

	// Send query with mixed-case QNAME to verify case-insensitive RFC 4343 matching.
	query := buildTestMessage(t, dnsmessage.Header{
		ID:               0xbeef,
		RecursionDesired: true,
	}, []dnsmessage.Question{{
		Name:  dnsmessage.MustNewName("ExAmPlE.CoM."),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}})

	act := h.onRequest(query)
	if act.kind != actionForward {
		t.Fatalf("onRequest kind = %v, want actionForward", act.kind)
	}
	if act.clientRequestID != 0xbeef {
		t.Fatalf("clientRequestID = %#x, want 0xbeef", act.clientRequestID)
	}
	if gotName := act.question.Name.String(); gotName != "example.com." {
		t.Fatalf("canonical question name = %q, want %q", gotName, "example.com.")
	}

	upstream1 := &net.UDPAddr{IP: net.ParseIP("10.96.0.10"), Port: 53}
	upstream2 := &net.UDPAddr{IP: net.ParseIP("10.96.0.11"), Port: 53}
	wrongUpstream := &net.UDPAddr{IP: net.ParseIP("10.96.0.99"), Port: 53}
	clientAddr := &net.UDPAddr{IP: net.ParseIP("169.254.0.2"), Port: 54321}

	rewritten := bytes.Clone(query)
	upstreamID, ok := pending.record(rewritten, act.clientRequestID, act.question, clientAddr, []string{upstream1.String(), upstream2.String()}, act.hasSlot)
	if !ok {
		t.Fatal("pending.record failed")
	}

	// 1. Response with QR == 0 is dropped.
	if res := h.onResponse(rewritten, upstream1); res.kind != actionDrop {
		t.Errorf("QR=0 response kind = %v, want actionDrop", res.kind)
	}

	// 2. Response from unexpected upstream address is dropped without evicting pending request.
	validResp := dnsAnswer(rewritten, 0)
	if res := h.onResponse(validResp, wrongUpstream); res.kind != actionDrop {
		t.Errorf("wrong upstream source kind = %v, want actionDrop", res.kind)
	}

	// 3. Response with wrong transaction ID is dropped.
	wrongIDResp := bytes.Clone(validResp)
	binary.BigEndian.PutUint16(wrongIDResp[0:2], upstreamID^0xffff)
	if res := h.onResponse(wrongIDResp, upstream1); res.kind != actionDrop {
		t.Errorf("wrong ID response kind = %v, want actionDrop", res.kind)
	}

	// 4. Response with mismatched QNAME is dropped.
	wrongQNameResp := buildTestMessage(t, dnsmessage.Header{
		ID:       upstreamID,
		Response: true,
	}, []dnsmessage.Question{{
		Name:  dnsmessage.MustNewName("other.com."),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}})
	if res := h.onResponse(wrongQNameResp, upstream1); res.kind != actionDrop {
		t.Errorf("wrong QNAME response kind = %v, want actionDrop", res.kind)
	}

	// 5. Response with mismatched QTYPE is dropped.
	wrongQTypeResp := buildTestMessage(t, dnsmessage.Header{
		ID:       upstreamID,
		Response: true,
	}, []dnsmessage.Question{{
		Name:  dnsmessage.MustNewName("example.com."),
		Type:  dnsmessage.TypeAAAA,
		Class: dnsmessage.ClassINET,
	}})
	if res := h.onResponse(wrongQTypeResp, upstream1); res.kind != actionDrop {
		t.Errorf("wrong QTYPE response kind = %v, want actionDrop", res.kind)
	}

	// 6. SERVFAIL from upstream1 triggers ActionFailover to upstream2.
	servFailResp := dnsAnswer(rewritten, byte(dnsmessage.RCodeServerFailure))
	failoverAct := h.onResponse(servFailResp, upstream1)
	if failoverAct.kind != actionFailover {
		t.Fatalf("SERVFAIL on first upstream kind = %v, want actionFailover", failoverAct.kind)
	}
	if failoverAct.upstreamIdx != 1 {
		t.Errorf("failover upstreamIdx = %d, want 1", failoverAct.upstreamIdx)
	}
	if len(lim.inFlight) != 1 {
		t.Errorf("inFlight slots during failover = %d, want 1", len(lim.inFlight))
	}

	// 7. Valid response from upstream2 (with different QNAME casing) delivers and restores clientRequestID.
	lowerResp := buildTestMessage(t, dnsmessage.Header{
		ID:       upstreamID,
		Response: true,
		RCode:    dnsmessage.RCodeSuccess,
	}, []dnsmessage.Question{{
		Name:  dnsmessage.MustNewName("example.com."),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}})
	deliverAct := h.onResponse(lowerResp, upstream2)
	if deliverAct.kind != actionDeliver {
		t.Fatalf("valid response on second upstream kind = %v, want actionDeliver", deliverAct.kind)
	}
	if gotID := binary.BigEndian.Uint16(deliverAct.payload[0:2]); gotID != 0xbeef {
		t.Errorf("delivered ID = %#x, want 0xbeef", gotID)
	}
	if deliverAct.clientSource != clientAddr {
		t.Errorf("delivered clientSource = %v, want %v", deliverAct.clientSource, clientAddr)
	}
	if len(lim.inFlight) != 0 {
		t.Errorf("inFlight slots after delivery = %d, want 0", len(lim.inFlight))
	}
}
