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
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"golang.org/x/net/dns/dnsmessage"
)

// exampleQuestion is the question dnsQuery asks.
var exampleQuestion = dnsmessage.Question{
	Name:  dnsmessage.MustNewName("example.com."),
	Type:  dnsmessage.TypeA,
	Class: dnsmessage.ClassINET,
}

// recordQuery records dnsQuery(clientID) holding an in-flight slot, as a
// forwarded query does, and returns its upstream ID and the rewritten query.
func recordQuery(t *testing.T, p *pendingRequests, clientID uint16, client any, upstreams []string) (uint16, []byte) {
	t.Helper()
	p.limiter.inFlight <- struct{}{}
	raw := dnsQuery(clientID)
	id, ok := p.record(raw, clientID, exampleQuestion, client, upstreams, true)
	if !ok {
		t.Fatalf("record(%#x) failed", clientID)
	}
	return id, raw
}

// checkExpiry fails t unless expiry is timeout after an instant in [before, after].
func checkExpiry(t *testing.T, expiry, before, after time.Time, timeout time.Duration) {
	t.Helper()
	if expiry.Before(before.Add(timeout)) || expiry.After(after.Add(timeout)) {
		t.Errorf("expiry is %v after the call, want %v", expiry.Sub(before), timeout)
	}
}

func TestPendingRequestsTimeoutForAttempt(t *testing.T) {
	// Long enough that splitting it two ways still exceeds the cap.
	const exchange = 10 * defaultUpstreamAttemptTimeout

	for _, tc := range []struct {
		name            string
		exchangeTimeout time.Duration
		attemptTimeout  time.Duration
		upstreamIdx     int
		numUpstreams    int
		want            time.Duration
	}{
		{
			name:            "single upstream gets the whole exchange",
			exchangeTimeout: exchange,
			numUpstreams:    1,
			want:            exchange,
		},
		{
			name:            "attempt with fallbacks left is capped",
			exchangeTimeout: exchange,
			numUpstreams:    2,
			want:            defaultUpstreamAttemptTimeout,
		},
		{
			name:            "attempt with fallbacks left gets its share",
			exchangeTimeout: defaultUpstreamAttemptTimeout,
			upstreamIdx:     1,
			numUpstreams:    4,
			want:            defaultUpstreamAttemptTimeout / 4,
		},
		{
			name:            "last upstream gets the whole exchange",
			exchangeTimeout: exchange,
			upstreamIdx:     1,
			numUpstreams:    2,
			want:            exchange,
		},
		{
			name:         "unset exchange timeout uses the default",
			numUpstreams: 1,
			want:         dnsExchangeTimeout,
		},
		{
			name:            "attempt timeout overrides an attempt with fallbacks left",
			exchangeTimeout: exchange,
			attemptTimeout:  40 * time.Millisecond,
			numUpstreams:    2,
			want:            40 * time.Millisecond,
		},
		{
			name:            "attempt timeout overrides the last attempt",
			exchangeTimeout: exchange,
			attemptTimeout:  40 * time.Millisecond,
			upstreamIdx:     1,
			numUpstreams:    2,
			want:            40 * time.Millisecond,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPendingRequests(nil)
			p.exchangeTimeout = tc.exchangeTimeout
			p.attemptTimeout = tc.attemptTimeout
			if got := p.timeoutForAttempt(tc.upstreamIdx, tc.numUpstreams); got != tc.want {
				t.Errorf("timeoutForAttempt(%d, %d) = %v, want %v", tc.upstreamIdx, tc.numUpstreams, got, tc.want)
			}
		})
	}
}

func TestPendingRequestsRecord(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	client := &net.UDPAddr{IP: net.ParseIP("169.254.0.2"), Port: 54321}
	// Responses are matched on their unmapped source address, so the key must
	// be normalized.
	upstreams := []string{"[::ffff:10.96.0.10]:53", "10.96.0.11:53"}

	lim.inFlight <- struct{}{}
	raw := dnsQuery(0xbeef)
	before := time.Now()
	id, ok := p.record(raw, 0xbeef, exampleQuestion, client, upstreams, true)
	after := time.Now()
	if !ok {
		t.Fatal("record failed")
	}
	if got := binary.BigEndian.Uint16(raw[0:2]); got != id {
		t.Errorf("query ID = %#x, want the upstream ID %#x", got, id)
	}
	entry, ok := p.entries[pendingKey{upstreamID: id, transportSource: "10.96.0.10:53"}]
	if !ok {
		t.Fatal("no entry keyed by the upstream ID and the first upstream's normalized address")
	}
	want := &pendingRequest{
		clientRequestID: 0xbeef,
		clientSource:    client,
		question:        exampleQuestion,
		rawQuery:        raw,
		upstreams:       upstreams,
		hasSlot:         true,
	}
	if diff := cmp.Diff(want, entry, cmp.AllowUnexported(pendingRequest{}), cmpopts.IgnoreFields(pendingRequest{}, "expiry")); diff != "" {
		t.Errorf("entry mismatch (-want +got):\n%s", diff)
	}
	checkExpiry(t, entry.expiry, before, after, p.timeoutForAttempt(0, len(upstreams)))
	if _, ok := p.inUse[id]; !ok {
		t.Errorf("upstream ID %#x is not marked in use", id)
	}
	if got := len(lim.inFlight); got != 1 {
		t.Errorf("%d in-flight slots held, want 1 until the request completes", got)
	}

	// The entry keeps its own copy of the query to resend on failover.
	raw[len(raw)-1] ^= 0xff
	if bytes.Equal(entry.rawQuery, raw) {
		t.Error("entry shares the caller's query buffer")
	}
}

func TestPendingRequestsRecordRejects(t *testing.T) {
	for _, tc := range []struct {
		name      string
		raw       []byte
		upstreams []string
		hasSlot   bool
	}{
		{
			name:      "query too short to carry an ID",
			raw:       []byte{0x12},
			upstreams: []string{"10.96.0.10:53"},
			hasSlot:   true,
		},
		{
			name:    "no upstreams",
			raw:     dnsQuery(0x1234),
			hasSlot: true,
		},
		{
			name: "no upstreams and no slot held",
			raw:  dnsQuery(0x1234),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lim := newLimiter()
			p := newPendingRequests(lim)
			lim.inFlight <- struct{}{} // held by another query
			if tc.hasSlot {
				lim.inFlight <- struct{}{}
			}
			if _, ok := p.record(tc.raw, 0x1234, exampleQuestion, nil, tc.upstreams, tc.hasSlot); ok {
				t.Fatal("record succeeded")
			}
			if got := len(lim.inFlight); got != 1 {
				t.Errorf("%d in-flight slots held, want 1: the other query's", got)
			}
			if len(p.entries) != 0 || len(p.inUse) != 0 {
				t.Errorf("rejected query left %d entries and %d IDs in use", len(p.entries), len(p.inUse))
			}
		})
	}
}

// Responses are matched by upstream ID, so record must not hand out one still
// in flight.
func TestPendingRequestsRecordAllocatesFreeID(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	upstreams := []string{"10.96.0.10:53"}
	const free = 0xabcd
	for id := range 1 << 16 {
		if id != free {
			p.inUse[uint16(id)] = struct{}{}
		}
	}

	if id, _ := recordQuery(t, p, 0x1234, nil, upstreams); id != free {
		t.Fatalf("record allocated %#x, want the only free ID %#x", id, free)
	}

	// With every ID in flight, record fails and frees the query's slot.
	lim.inFlight <- struct{}{}
	if _, ok := p.record(dnsQuery(0x5678), 0x5678, exampleQuestion, nil, upstreams, true); ok {
		t.Fatal("record succeeded with every upstream ID in flight")
	}
	if got := len(lim.inFlight); got != 1 {
		t.Errorf("%d in-flight slots held, want 1 for the recorded query", got)
	}
}

func TestPendingRequestsDeleteEntryReleasesSlotOnce(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	upstreams := []string{"10.96.0.10:53"}
	lim.inFlight <- struct{}{} // held by another query
	id, _ := recordQuery(t, p, 0x1234, nil, upstreams)
	key := pendingKey{upstreamID: id, transportSource: upstreams[0]}
	entry := p.entries[key]

	p.mu.Lock()
	p.deleteEntryLocked(key, entry)
	p.deleteEntryLocked(key, entry)
	p.mu.Unlock()

	if len(p.entries) != 0 || len(p.inUse) != 0 {
		t.Errorf("deleted entry left %d entries and %d IDs in use", len(p.entries), len(p.inUse))
	}
	if got := len(lim.inFlight); got != 1 {
		t.Errorf("%d in-flight slots held, want 1: deleting twice must free the slot once", got)
	}
}

func TestPendingRequestsFailOverOnSendError(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	upstreams := []string{"10.96.0.10:53", "10.96.0.11:53"}
	id, raw := recordQuery(t, p, 0x1234, nil, upstreams)

	// Errors for an unknown ID, or for an attempt not yet made, are ignored.
	if _, _, ok := p.failOverOnSendError(id+1, 0); ok {
		t.Error("failed over a request that was never recorded")
	}
	if _, _, ok := p.failOverOnSendError(id, 1); ok {
		t.Error("failed over on an error for an attempt not yet made")
	}

	// The first upstream failing moves the request to the second.
	before := time.Now()
	nextIdx, query, ok := p.failOverOnSendError(id, 0)
	after := time.Now()
	if !ok || nextIdx != 1 {
		t.Fatalf("failOverOnSendError(%#x, 0) = %d, _, %v; want 1, _, true", id, nextIdx, ok)
	}
	if !bytes.Equal(query, raw) {
		t.Error("failover query differs from the recorded query")
	}
	if _, ok := p.entries[pendingKey{upstreamID: id, transportSource: upstreams[0]}]; ok {
		t.Error("entry still keyed by the failed upstream")
	}
	entry, ok := p.entries[pendingKey{upstreamID: id, transportSource: upstreams[1]}]
	if !ok {
		t.Fatal("entry not keyed by the next upstream, so its answer would be dropped")
	}
	if entry.upstreamIdx != 1 {
		t.Errorf("upstreamIdx = %d, want 1", entry.upstreamIdx)
	}
	checkExpiry(t, entry.expiry, before, after, p.timeoutForAttempt(1, len(upstreams)))
	if got := len(lim.inFlight); got != 1 {
		t.Errorf("%d in-flight slots held during failover, want 1", got)
	}

	// A repeated error for the first attempt is stale.
	if _, _, ok := p.failOverOnSendError(id, 0); ok {
		t.Error("failed over twice on errors for the same attempt")
	}

	// The last upstream failing drops the request and frees its slot.
	if _, _, ok := p.failOverOnSendError(id, 1); ok {
		t.Error("failed over past the last upstream")
	}
	if len(p.entries) != 0 || len(p.inUse) != 0 {
		t.Errorf("dropped request left %d entries and %d IDs in use", len(p.entries), len(p.inUse))
	}
	if got := len(lim.inFlight); got != 0 {
		t.Errorf("%d in-flight slots held after the last upstream failed, want 0", got)
	}
}

func TestPendingRequestsByClientSource(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	upstreams := []string{"10.96.0.10:53"}
	clientA := &net.UDPAddr{IP: net.ParseIP("169.254.0.2"), Port: 1111}
	clientB := &net.UDPAddr{IP: net.ParseIP("169.254.0.2"), Port: 2222}
	recordQuery(t, p, 0x1111, clientA, upstreams)
	recordQuery(t, p, 0x2222, clientA, upstreams)
	idB, _ := recordQuery(t, p, 0x3333, clientB, upstreams)

	if got := p.countByClientSource(clientA); got != 2 {
		t.Errorf("countByClientSource(clientA) = %d, want 2", got)
	}
	if got := p.countByClientSource(clientB); got != 1 {
		t.Errorf("countByClientSource(clientB) = %d, want 1", got)
	}

	p.removeByClientSource(clientA)
	if got := p.countByClientSource(clientA); got != 0 {
		t.Errorf("countByClientSource(clientA) = %d after removing its requests, want 0", got)
	}
	if got := p.countByClientSource(clientB); got != 1 {
		t.Errorf("countByClientSource(clientB) = %d after removing clientA's requests, want 1", got)
	}
	if _, ok := p.inUse[idB]; !ok || len(p.inUse) != 1 {
		t.Errorf("%d IDs in use, want only clientB's %#x", len(p.inUse), idB)
	}
	if got := len(lim.inFlight); got != 1 {
		t.Errorf("%d in-flight slots held, want 1 for clientB's request", got)
	}
}

func TestPendingRequestsClearAll(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	upstreams := []string{"10.96.0.10:53", "10.96.0.11:53"}
	for i := range 3 {
		recordQuery(t, p, uint16(i), nil, upstreams)
	}

	p.clearAll()
	if len(p.entries) != 0 || len(p.inUse) != 0 {
		t.Errorf("clearAll left %d entries and %d IDs in use", len(p.entries), len(p.inUse))
	}
	if got := len(lim.inFlight); got != 0 {
		t.Errorf("%d in-flight slots held after clearAll, want 0", got)
	}
}

func TestPendingRequestsSweep(t *testing.T) {
	lim := newLimiter()
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
	wantFailovers := []reqFailover{{upstreamID: id, upstreamIdx: 1, query: raw}}
	if diff := cmp.Diff(wantFailovers, failovers, cmp.AllowUnexported(reqFailover{})); diff != "" {
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
	lim := newLimiter()
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
	wantDeliveries := []reqDelivery{{
		clientAddr: client,
		payload:    dnsAnswer(dnsQuery(0x1234), byte(dnsmessage.RCodeServerFailure)),
	}}
	if diff := cmp.Diff(wantDeliveries, deliveries, cmp.AllowUnexported(reqDelivery{})); diff != "" {
		t.Errorf("deliveries mismatch (-want +got):\n%s", diff)
	}
	if len(p.entries) != 0 || len(p.inUse) != 0 {
		t.Errorf("expired request left %d entries and %d IDs in use", len(p.entries), len(p.inUse))
	}
	if got := len(lim.inFlight); got != 0 {
		t.Errorf("%d in-flight slots held after expiry, want 0", got)
	}
}
