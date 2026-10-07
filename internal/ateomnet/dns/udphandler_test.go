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
	"context"
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/net/dns/dnsmessage"
)

func startTestUDPHandler(t *testing.T, upstreams []string, configure func(*udpHandler, *dnsHandler)) net.Conn {
	t.Helper()
	ingress, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	egress, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		ingress.Close()
		t.Fatal(err)
	}

	udpAddrs := make([]*net.UDPAddr, len(upstreams))
	for i, u := range upstreams {
		addr, err := net.ResolveUDPAddr("udp", u)
		if err != nil {
			ingress.Close()
			egress.Close()
			t.Fatal(err)
		}
		udpAddrs[i] = addr
	}

	lim := &limiter{
		inFlight:    make(chan struct{}, maxInFlightDNS),
		connections: make(chan struct{}, maxDNSConnections),
	}
	pending := newPendingRequests(lim)
	dnsH := newDNSHandler(pending, lim, nil)
	h := newUDPHandler(ingress, egress, udpAddrs, dnsH, pending)
	if configure != nil {
		configure(h, dnsH)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.serve(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = ingress.Close()
		_ = egress.Close()
		<-done
	})

	client, err := net.Dial("udp", ingress.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestUDPHandlerForwardsAndRestoresID(t *testing.T) {
	var seenUpstreamID atomic.Uint32
	upstream := newFakeResolver(t, func(query []byte) []byte {
		seenUpstreamID.Store(uint32(binary.BigEndian.Uint16(query[0:2])))
		return dnsAnswer(query, 0)
	})

	client := startTestUDPHandler(t, []string{upstream}, nil)
	const clientID = 0x4242
	query := dnsQuery(clientID)
	if _, err := client.Write(query); err != nil {
		t.Fatal(err)
	}
	got := readWithin(t, client)
	if gotID := binary.BigEndian.Uint16(got[0:2]); gotID != clientID {
		t.Errorf("response ID = %#x, want %#x", gotID, clientID)
	}
	if got[2]&0x80 == 0 {
		t.Error("expected response bit QR=1 to be set")
	}
}

func TestUDPHandlerSynthesizedReply(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := newFakeResolver(t, func(query []byte) []byte {
		upstreamCalls.Add(1)
		return dnsAnswer(query, 0)
	})

	client := startTestUDPHandler(t, []string{upstream}, func(_ *udpHandler, dnsH *dnsHandler) {
		dnsH.allow = func(dnsmessage.Question) bool { return false }
	})

	query := dnsQuery(0x9999)
	if _, err := client.Write(query); err != nil {
		t.Fatal(err)
	}
	got := readWithin(t, client)
	if rcode := dnsmessage.RCode(got[3] & 0x0f); rcode != dnsmessage.RCodeRefused {
		t.Errorf("synthesized rcode = %v, want %v", rcode, dnsmessage.RCodeRefused)
	}
	if upstreamCalls.Load() != 0 {
		t.Errorf("upstream was called %d times on synthesized reply, want 0", upstreamCalls.Load())
	}
}

func TestUDPHandlerFailoverOnSweepTimeoutAndDeferredDelivery(t *testing.T) {
	silent1, _ := newSilentResolver(t)
	healthy := newFakeResolver(t, func(query []byte) []byte {
		return dnsAnswer(query, 0)
	})

	client := startTestUDPHandler(t, []string{silent1, healthy}, func(h *udpHandler, _ *dnsHandler) {
		h.pending.attemptTimeout = 40 * time.Millisecond
		h.sweepInterval = 10 * time.Millisecond
	})

	if _, err := client.Write(dnsQuery(0x7777)); err != nil {
		t.Fatal(err)
	}
	got := readWithin(t, client)
	if gotID := binary.BigEndian.Uint16(got[0:2]); gotID != 0x7777 {
		t.Errorf("response ID = %#x, want 0x7777", gotID)
	}
	if rcode := got[3] & 0x0f; rcode != 0 {
		t.Errorf("rcode = %d, want 0 after timeout failover", rcode)
	}

	// Also verify that if the first upstream returns SERVFAIL and the second
	// upstream times out, the deferred SERVFAIL is delivered on expiry.
	sick := newFakeResolver(t, func(query []byte) []byte {
		return dnsAnswer(query, byte(dnsmessage.RCodeServerFailure))
	})
	silent2, _ := newSilentResolver(t)
	client2 := startTestUDPHandler(t, []string{sick, silent2}, func(h *udpHandler, _ *dnsHandler) {
		h.pending.attemptTimeout = 40 * time.Millisecond
		h.sweepInterval = 10 * time.Millisecond
	})

	if _, err := client2.Write(dnsQuery(0x8888)); err != nil {
		t.Fatal(err)
	}
	got2 := readWithin(t, client2)
	if gotID := binary.BigEndian.Uint16(got2[0:2]); gotID != 0x8888 {
		t.Errorf("deferred response ID = %#x, want 0x8888", gotID)
	}
	if rcode := dnsmessage.RCode(got2[3] & 0x0f); rcode != dnsmessage.RCodeServerFailure {
		t.Errorf("deferred rcode = %v, want %v", rcode, dnsmessage.RCodeServerFailure)
	}
}

func TestPendingRequestsSweep(t *testing.T) {
	lim := newTestLimiter()
	p := newPendingRequests(lim)
	upstreams := []string{"10.96.0.10:53", "10.96.0.11:53"}
	id, raw := recordQuery(t, p, 0x1234, nil, upstreams)
	entry := p.entries[pendingKey{upstreamID: id, transportSource: upstreams[0]}]

	if failovers, deliveries := p.sweep(entry.expiry); len(failovers) != 0 || len(deliveries) != 0 {
		t.Fatalf("sweep at the expiry returned %d failovers and %d deliveries, want none until it has passed", len(failovers), len(deliveries))
	}

	// An expired attempt moves to the next upstream.
	now := entry.expiry.Add(time.Nanosecond)
	failovers, deliveries := p.sweep(now)
	wantFailovers := []udpFailover{{upstreamID: id, upstreamIdx: 1, query: raw}}
	if diff := cmp.Diff(wantFailovers, failovers, cmp.AllowUnexported(udpFailover{})); diff != "" {
		t.Errorf("failovers mismatch (-want +got):\n%s", diff)
	}
	if len(deliveries) != 0 {
		t.Errorf("sweep returned %d deliveries on failover, want 0", len(deliveries))
	}
	if len(p.entries) != 1 || p.entries[pendingKey{upstreamID: id, transportSource: upstreams[1]}] != entry {
		t.Fatal("entry not re-keyed to the next upstream")
	}
	if want := now.Add(p.timeoutForAttempt(1, len(upstreams))); !entry.expiry.Equal(want) {
		t.Errorf("expiry = %v, want %v", entry.expiry, want)
	}

	// An expired last attempt with no answer held back is dropped silently.
	failovers, deliveries = p.sweep(entry.expiry.Add(time.Nanosecond))
	if len(failovers) != 0 || len(deliveries) != 0 {
		t.Errorf("sweep after the last attempt returned %d failovers and %d deliveries, want none", len(failovers), len(deliveries))
	}
	if len(p.entries) != 0 || len(p.inUse) != 0 {
		t.Errorf("expired request left %d entries and %d IDs in use", len(p.entries), len(p.inUse))
	}
	if got := len(lim.inFlight); got != 0 {
		t.Errorf("%d in-flight slots held after expiry, want 0", got)
	}
}

// A SERVFAIL beats a timeout: when the last upstream never answers, the client
// gets the SERVFAIL held back from an earlier one.
func TestPendingRequestsSweepDeliversDeferredAnswer(t *testing.T) {
	lim := newTestLimiter()
	p := newPendingRequests(lim)
	client := &net.UDPAddr{IP: net.ParseIP("169.254.0.2"), Port: 54321}
	upstreams := []string{"10.96.0.10:53", "10.96.0.11:53"}
	id, raw := recordQuery(t, p, 0x1234, client, upstreams)

	// Leave the request as a SERVFAIL from the first upstream does: waiting on
	// the second, with the SERVFAIL held back.
	if _, _, ok := p.failOverOnSendError(id, 0); !ok {
		t.Fatal("failOverOnSendError failed")
	}
	entry := p.entries[pendingKey{upstreamID: id, transportSource: upstreams[1]}]
	entry.deferredResp = dnsAnswer(raw, byte(dnsmessage.RCodeServerFailure))

	failovers, deliveries := p.sweep(entry.expiry.Add(time.Nanosecond))
	if len(failovers) != 0 {
		t.Errorf("sweep returned %d failovers past the last upstream, want 0", len(failovers))
	}
	wantDeliveries := []udpDelivery{{
		clientAddr: client,
		payload:    dnsAnswer(dnsQuery(0x1234), byte(dnsmessage.RCodeServerFailure)),
	}}
	if diff := cmp.Diff(wantDeliveries, deliveries, cmp.AllowUnexported(udpDelivery{})); diff != "" {
		t.Errorf("deliveries mismatch (-want +got):\n%s", diff)
	}
	if len(p.entries) != 0 || len(p.inUse) != 0 {
		t.Errorf("expired request left %d entries and %d IDs in use", len(p.entries), len(p.inUse))
	}
	if got := len(lim.inFlight); got != 0 {
		t.Errorf("%d in-flight slots held after expiry, want 0", got)
	}
}
