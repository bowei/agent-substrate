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
	"fmt"
	"net"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet/dns"
)

// harness runs one [dns.Relay] against fake upstreams. Like a worker, it
// serves any number of sandboxes from that one relay, so they share its
// limits.
type harness struct {
	tb        testing.TB
	relay     *dns.Relay
	upstreams []*upstream
}

// newHarness starts one fake upstream per behavior, in the order the relay
// tries them, and a relay forwarding to them. Everything is torn down when
// the test ends.
func newHarness(tb testing.TB, upstreams ...behavior) *harness {
	tb.Helper()
	if len(upstreams) == 0 {
		upstreams = []behavior{{}}
	}
	h := &harness{tb: tb}
	var addrs []string
	for _, b := range upstreams {
		u, err := startUpstream(b)
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() { _ = u.close() })
		h.upstreams = append(h.upstreams, u)
		addrs = append(addrs, u.addr())
	}
	relay, err := dns.NewRelayForUpstreams(addrs)
	if err != nil {
		tb.Fatal(err)
	}
	h.relay = relay
	return h
}

// sandbox is the relay serving one simulated sandbox on loopback sockets.
type sandbox struct {
	endpoint endpoint
	server   *dns.Server
}

// addSandbox serves the relay on new loopback sockets through
// [dns.Relay.ServeOn], the code path [dns.Relay.Serve] uses after binding in
// the sandbox's namespace.
func (h *harness) addSandbox() *sandbox {
	h.tb.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		h.tb.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = pc.Close()
		h.tb.Fatal(err)
	}
	sb := &sandbox{
		endpoint: endpoint{udp: pc.LocalAddr().String(), tcp: l.Addr().String()},
		server:   h.relay.ServeOn(context.Background(), pc, l),
	}
	h.tb.Cleanup(func() { _ = sb.close() })
	return sb
}

// close tears the sandbox down and waits for serving to stop.
func (sb *sandbox) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return sb.server.Stop(ctx)
}

// runLoad sends load to sb until ctx is done and returns what it observed.
// load.Sandbox, StartAfter and StopAfter are ignored.
func (sb *sandbox) runLoad(ctx context.Context, load load) loadStats {
	return runLoad(ctx, load, sb.endpoint)
}

// phase changes an upstream's behavior partway through a scenario.
type phase struct {
	at       time.Duration
	upstream int
	behavior behavior
}

// thresholds are what a scenario is expected to achieve. They are reported,
// not enforced: a miss is logged as a finding and does not fail the test.
type thresholds struct {
	// loads names the loads the thresholds apply to; empty means all.
	loads []string
	// minAnswered is the least fraction of sent queries answered.
	minAnswered float64
	// maxP99 is the largest acceptable 99th percentile latency.
	maxP99 time.Duration
}

// scenario is one stress run: upstream behaviors, sandboxes on a shared
// relay, the load each sandbox receives, and changes made while it runs.
type scenario struct {
	name      string
	upstreams []behavior
	sandboxes int
	loads     []load
	duration  time.Duration
	phases    []phase
	expect    thresholds
}

// run executes s and returns its report. Leaks and a relay that keeps serving
// after teardown fail tb; everything else is left to the caller.
func run(tb testing.TB, s scenario) *report {
	tb.Helper()
	h := newHarness(tb, s.upstreams...)
	return h.run(s)
}

// run executes s on h's relay and upstreams. s.upstreams is ignored.
func (h *harness) run(s scenario) *report {
	tb := h.tb
	tb.Helper()
	for i, l := range s.loads {
		if l.Sandbox < 0 || l.Sandbox >= max(s.sandboxes, 1) {
			tb.Fatalf("load %d (%s) targets sandbox %d of %d", i, l.Name, l.Sandbox, s.sandboxes)
		}
	}

	baseline := settledResources()
	for _, u := range h.upstreams {
		u.resetPeak()
	}
	upstreamBefore := make([]upstreamStats, len(h.upstreams))
	for i, u := range h.upstreams {
		upstreamBefore[i] = u.stats()
	}

	sandboxes := make([]*sandbox, max(s.sandboxes, 1))
	for i := range sandboxes {
		sandboxes[i] = h.addSandbox()
	}
	smp := startSampler(100 * time.Millisecond)
	start := time.Now()

	for _, p := range s.phases {
		t := time.AfterFunc(p.at, func() { h.upstreams[p.upstream].setBehavior(p.behavior) })
		defer t.Stop()
	}

	results := make([]loadStats, len(s.loads))
	var wg sync.WaitGroup
	for i, l := range s.loads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sleepUntil(start.Add(l.StartAfter))
			stop := s.duration
			if l.StopAfter > 0 && l.StopAfter < stop {
				stop = l.StopAfter
			}
			ctx, cancel := context.WithDeadline(context.Background(), start.Add(stop))
			defer cancel()
			results[i] = sandboxes[l.Sandbox].runLoad(ctx, l)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	peak := smp.stop()

	r := &report{
		Name:        s.name,
		Duration:    s.duration,
		Elapsed:     elapsed,
		Sandboxes:   len(sandboxes),
		Baseline:    baseline,
		Peak:        peak,
		RaceEnabled: raceEnabled,
	}
	for i, l := range s.loads {
		r.Loads = append(r.Loads, loadReport{load: l, stats: results[i]})
	}
	for i, u := range h.upstreams {
		r.Upstreams = append(r.Upstreams, u.stats().since(upstreamBefore[i]))
	}

	for i, sb := range sandboxes {
		if err := sb.close(); err != nil {
			tb.Errorf("%s: closing sandbox %d: %v", s.name, i, err)
		}
	}
	after, ok := waitForBaseline(baseline, 5*time.Second)
	r.After = after
	if !ok {
		tb.Errorf("%s: leak after teardown: goroutines %d -> %d, fds %d -> %d",
			s.name, baseline.Goroutines, after.Goroutines, baseline.FDs, after.FDs)
	}
	r.Findings = s.expect.check(r)
	return r
}

// settledResources samples once the goroutines of earlier work have exited.
func settledResources() resources {
	runtime.GC() // so heap figures do not carry an earlier scenario's garbage
	prev := sampleResources()
	for range 50 {
		time.Sleep(20 * time.Millisecond)
		now := sampleResources()
		if now.Goroutines == prev.Goroutines && now.FDs == prev.FDs {
			return now
		}
		prev = now
	}
	return prev
}

func sleepUntil(t time.Time) {
	if d := time.Until(t); d > 0 {
		time.Sleep(d)
	}
}

func (s upstreamStats) since(before upstreamStats) upstreamStats {
	return upstreamStats{
		Queries:      s.Queries - before.Queries,
		Answered:     s.Answered - before.Answered,
		Dropped:      s.Dropped - before.Dropped,
		TCPConns:     s.TCPConns - before.TCPConns,
		InFlight:     s.InFlight,
		PeakInFlight: s.PeakInFlight,
	}
}

func (t thresholds) check(r *report) []string {
	var findings []string
	for _, lr := range r.Loads {
		if len(t.loads) > 0 && !slices.Contains(t.loads, lr.load.Name) {
			continue
		}
		if lr.stats.Sent == 0 {
			continue
		}
		if t.minAnswered > 0 && lr.stats.answeredFraction() < t.minAnswered {
			findings = append(findings, fmt.Sprintf("%s: answered %.2f%%, want >= %.2f%%",
				lr.load.Name, 100*lr.stats.answeredFraction(), 100*t.minAnswered))
		}
		if p99 := lr.stats.Latency.quantile(0.99); t.maxP99 > 0 && p99 > t.maxP99 {
			findings = append(findings, fmt.Sprintf("%s: p99 %v, want <= %v", lr.load.Name, p99, t.maxP99))
		}
	}
	return findings
}
