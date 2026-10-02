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

package stress

import (
	"context"
	"net"
	"testing"
	"time"
)

// These tests cover the harness itself and run in every "go test", so the
// scenarios behind -stress do not rot.

func TestBuildAnswerRoundTrip(t *testing.T) {
	q := buildQuery(0x1234, "svc.cluster.local.", typeAAAA)
	for _, tc := range []struct {
		rcode, size, wantAtLeast int
	}{
		{rcodeSuccess, 0, len(q) + rrALen},
		{rcodeSuccess, 4000, 4000 - rrALen},
		{rcodeServFail, 4000, len(q)},
	} {
		resp, err := buildAnswer(q, tc.rcode, tc.size)
		if err != nil {
			t.Fatal(err)
		}
		rcode, err := checkAnswer(q, resp)
		if err != nil || rcode != tc.rcode {
			t.Errorf("checkAnswer(rcode %d, size %d) = %d, %v", tc.rcode, tc.size, rcode, err)
		}
		if len(resp) < tc.wantAtLeast || (tc.size > 0 && len(resp) > tc.size) && tc.rcode == rcodeSuccess {
			t.Errorf("rcode %d size %d: answer is %d bytes", tc.rcode, tc.size, len(resp))
		}
	}
	other := buildQuery(0x1234, "example.com", typeA)
	resp, _ := buildAnswer(other, rcodeSuccess, 0)
	if _, err := checkAnswer(q, resp); err == nil {
		t.Error("checkAnswer accepted the answer to another question")
	}
}

func TestHistogramQuantiles(t *testing.T) {
	var h histogram
	for i := 1; i <= 1000; i++ {
		h.record(time.Duration(i) * time.Millisecond)
	}
	for _, tc := range []struct {
		q    float64
		want time.Duration
	}{{0.5, 500 * time.Millisecond}, {0.99, 990 * time.Millisecond}, {1, time.Second}} {
		got := h.quantile(tc.q)
		if got < tc.want || float64(got) > float64(tc.want)*1.125 {
			t.Errorf("quantile(%v) = %v, want within 12.5%% above %v", tc.q, got, tc.want)
		}
	}
	for us := int64(0); us < 1<<20; us = us*5/4 + 1 {
		if b := bucketOf(us); bucketUpper(b) < us || (b > 0 && bucketUpper(b-1) >= us) {
			t.Fatalf("%dµs is in bucket %d, which spans (%d, %d]", us, b, bucketUpper(b-1), bucketUpper(b))
		}
	}
}

func TestUpstreamBehaviors(t *testing.T) {
	u, err := startUpstream(behavior{rcode: rcodeNXDomain})
	if err != nil {
		t.Fatal(err)
	}
	defer u.close()
	conn, err := net.Dial("udp", u.addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ask := func() (int, error) {
		q := buildQuery(7, "example.com", typeA)
		if _, err := conn.Write(q); err != nil {
			return 0, err
		}
		_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		buf := make([]byte, 512)
		n, err := conn.Read(buf)
		if err != nil {
			return 0, err
		}
		return checkAnswer(q, buf[:n])
	}
	if rcode, err := ask(); err != nil || rcode != rcodeNXDomain {
		t.Errorf("NXDOMAIN upstream answered %d, %v", rcode, err)
	}
	u.setBehavior(behavior{dropFraction: 1})
	if _, err := ask(); !isTimeout(err) {
		t.Errorf("blackholed upstream: err = %v, want a timeout", err)
	}
	if s := u.stats(); s.Queries != 2 || s.Answered != 1 || s.Dropped != 1 {
		t.Errorf("stats = %+v, want 2 queries, 1 answered, 1 dropped", s)
	}
}

// TestScenarioSmoke runs a short scenario over UDP and TCP through the relay.
// The first upstream answers SERVFAIL: UDP fails over to the second, while
// TCP is spliced to the first reachable upstream and passes SERVFAIL through.
func TestScenarioSmoke(t *testing.T) {
	r := run(t, scenario{
		name:      "Smoke",
		upstreams: []behavior{{rcode: rcodeServFail}, {}},
		sandboxes: 2,
		loads: []load{
			{Name: "udp", Sandbox: 0, Proto: protoUDP, QPS: 200},
			{Name: "tcp", Sandbox: 1, Proto: protoTCP, Concurrency: 2, Pipeline: true, ReuseConn: true},
		},
		duration: 300 * time.Millisecond,
	})
	t.Log("\n" + r.String())
	for _, tc := range []struct{ load, rcode string }{{"udp", "NOERROR"}, {"tcp", "SERVFAIL"}} {
		s := r.load(tc.load).stats
		if s.Sent == 0 || s.Answered != s.Sent || s.Rcodes[tc.rcode] != s.Answered || s.Malformed != 0 {
			t.Errorf("%s: sent %d, answered %d, malformed %d, rcodes %v: want every query answered %s",
				tc.load, s.Sent, s.Answered, s.Malformed, s.Rcodes, tc.rcode)
		}
	}
	if r.Upstreams[1].Queries == 0 {
		t.Error("the second upstream was never tried")
	}
}

// TestIdleConnsHoldRelaySlots checks that IdleConns can exhaust the relay's
// TCP connections, which TCPConnectionExhaustion relies on.
func TestIdleConnsHoldRelaySlots(t *testing.T) {
	h := newHarness(t)
	sb := h.addSandbox()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan loadStats)
	go func() { done <- sb.runLoad(ctx, load{Proto: protoTCP, IdleConns: 20}) }()
	s := <-done
	if s.IdleHeld < 16 || s.Errors["eof"] == 0 {
		t.Errorf("idle held %d, refused %v: want the relay's 16 held and the rest refused", s.IdleHeld, s.Errors)
	}
}
