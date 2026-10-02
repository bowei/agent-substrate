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
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// See the package documentation for how to run these scenarios.
var (
	runStress      = flag.Bool("stress", false, "run the DNS relay stress scenarios (skipped otherwise)")
	stressDuration = flag.Duration("stress.duration", 10*time.Second, "length of each stress scenario")
	stressScale    = flag.Float64("stress.scale", 1, "multiplier for load QPS, concurrency and quiet-sandbox counts")
	stressSoak     = flag.Duration("stress.soak", 0, "also run the mixed-fault Soak scenario for this long (0 skips it)")
	stressReport   = flag.String("stress.report", "", "write every scenario's report to this path as JSON")
	relayLogs      = flag.Bool("stress.relaylogs", false, "show the relay's INFO and WARN logs (one per failed query under load)")
)

var (
	reportsMu sync.Mutex
	reports   []*report
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !*relayLogs {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	}
	code := m.Run()
	if *stressReport != "" && len(reports) > 0 {
		data, err := json.MarshalIndent(reports, "", "  ")
		if err == nil {
			err = os.WriteFile(*stressReport, data, 0o644)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "writing -stress.report: %v\n", err)
			code = 1
		}
	}
	os.Exit(code)
}

// requireStress skips t unless -stress was given.
func requireStress(t *testing.T) {
	t.Helper()
	if !*runStress {
		t.Skip("stress scenario; run with: go test ./internal/ateomnet/dns/stress -stress -v (see package doc)")
	}
}

// scale multiplies n by -stress.scale, keeping at least 1.
func scale(n int) int { return max(1, int(math.Round(float64(n)**stressScale))) }

func scaleQPS(q float64) float64 { return q * *stressScale }

// logReport logs r, keeps it for -stress.report, and fails t on malformed
// answers unless the scenario provokes them.
func logReport(t *testing.T, r *report, allowMalformed bool) {
	t.Helper()
	t.Log("\n" + r.String())
	reportsMu.Lock()
	reports = append(reports, r)
	reportsMu.Unlock()
	if allowMalformed {
		return
	}
	for _, lr := range r.Loads {
		if lr.stats.Malformed > 0 {
			t.Errorf("%s: %d answers did not match their query", lr.load.Name, lr.stats.Malformed)
		}
	}
}

func TestStress(t *testing.T) {
	requireStress(t)
	d := *stressDuration

	scenarios := []scenario{
		{
			// Baseline throughput and latency with a fast upstream.
			name:      "SteadyStateUDP",
			upstreams: []behavior{{}},
			loads:     []load{{Name: "steady", Proto: protoUDP, QPS: scaleQPS(500)}},
			duration:  d,
			expect:    thresholds{minAnswered: 0.999, maxP99: 50 * time.Millisecond},
		},
		{
			// 64 UDP slots against a 200±100ms upstream hold about 320 QPS;
			// the relay drops the excess silently.
			name:      "SlowUpstream",
			upstreams: []behavior{{delay: 150 * time.Millisecond, jitter: 100 * time.Millisecond}},
			loads:     []load{{Name: "slow", Proto: protoUDP, QPS: scaleQPS(500)}},
			duration:  d,
			expect:    thresholds{minAnswered: 0.99},
		},
		{
			// The first upstream never answers: every query waits out the
			// relay's 5s attempt before the second is tried, as long as the
			// client's own timeout.
			name:      "PrimaryBlackholed",
			upstreams: []behavior{{dropFraction: 1}, {}},
			loads:     []load{{Name: "failover", Proto: protoUDP, QPS: scaleQPS(100)}},
			duration:  d,
			expect:    thresholds{minAnswered: 0.99, maxP99: time.Second},
		},
		{
			// SERVFAIL from the first upstream fails over to the second.
			name:      "ServfailFailover",
			upstreams: []behavior{{rcode: rcodeServFail}, {}},
			loads:     []load{{Name: "servfail", Proto: protoUDP, QPS: scaleQPS(500)}},
			duration:  d,
			expect:    thresholds{minAnswered: 0.999, maxP99: 50 * time.Millisecond},
		},
		noisyNeighbor(d),
		{
			// One sandbox holds all 16 relay TCP connections idle for the
			// first half; another sandbox's TCP queries are measured during
			// and after.
			name:      "TCPConnectionExhaustion",
			upstreams: []behavior{{}},
			sandboxes: 2,
			loads: []load{
				{Name: "idle", Sandbox: 0, Proto: protoTCP, IdleConns: 16, StopAfter: d / 2},
				{Name: "during", Sandbox: 1, Proto: protoTCP, QPS: scaleQPS(20), Timeout: 2 * time.Second, StartAfter: time.Second, StopAfter: d / 2},
				{Name: "after", Sandbox: 1, Proto: protoTCP, QPS: scaleQPS(20), Timeout: 2 * time.Second, StartAfter: d/2 + time.Second},
			},
			duration: d,
			expect:   thresholds{loads: []string{"during", "after"}, minAnswered: 0.99},
		},
		{
			// A and AAAA pipelined on one connection (glibc use-vc) against
			// an upstream answering in 0-300ms.
			name:      "TCPPipelined",
			upstreams: []behavior{{jitter: 300 * time.Millisecond}},
			loads:     []load{{Name: "pipelined", Proto: protoTCP, Concurrency: min(scale(8), 16), Pipeline: true, ReuseConn: true}},
			duration:  d,
			expect:    thresholds{minAnswered: 0.999, maxP99: 350 * time.Millisecond},
		},
		{
			// ~4 KiB EDNS-sized answers.
			name:      "LargeAnswers",
			upstreams: []behavior{{answerSize: 4000, delay: 5 * time.Millisecond}},
			loads:     []load{{Name: "large", Proto: protoUDP, QPS: scaleQPS(1000)}},
			duration:  d,
			expect:    thresholds{minAnswered: 0.999, maxP99: 50 * time.Millisecond},
		},
		{
			// Every upstream is down and clients retry hard: the peak
			// goroutines, descriptors and heap of an outage.
			name:      "OutageMemory",
			upstreams: []behavior{{dropFraction: 1}, {dropFraction: 1}},
			loads:     []load{{Name: "flood", Proto: protoUDP, Concurrency: scale(256), Timeout: time.Second}},
			duration:  d,
		},
	}
	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			logReport(t, run(t, s), false)
		})
	}

	t.Run("SaturationUDP", testSaturationUDP)
	t.Run("ChurnTeardown", testChurnTeardown)
	t.Run("Soak", testSoak)
}

// noisyNeighbor floods the relay from one sandbox while quiet sandboxes send
// 5 QPS each; every sandbox shares the relay's 64 UDP slots.
func noisyNeighbor(d time.Duration) scenario {
	quiet := scale(4)
	s := scenario{
		name:      "NoisyNeighbor",
		upstreams: []behavior{{delay: 100 * time.Millisecond}},
		sandboxes: 1 + quiet,
		loads:     []load{{Name: "noisy", Sandbox: 0, Proto: protoUDP, Concurrency: scale(200)}},
		duration:  d,
	}
	for i := 1; i <= quiet; i++ {
		name := fmt.Sprintf("quiet-%d", i)
		s.loads = append(s.loads, load{Name: name, Sandbox: i, Proto: protoUDP, QPS: 5})
		s.expect.loads = append(s.expect.loads, name)
	}
	s.expect.minAnswered = 0.99
	s.expect.maxP99 = 250 * time.Millisecond
	return s
}

// testSaturationUDP raises the offered load in steps against a 10ms upstream
// to find where the relay starts dropping (64 slots / 10ms ≈ 6400 QPS).
func testSaturationUDP(t *testing.T) {
	step := max(*stressDuration/5, time.Second)
	h := newHarness(t, behavior{delay: 10 * time.Millisecond})
	var summary strings.Builder
	summary.WriteString("offered_qps  answered%  p99\n")
	for _, qps := range []float64{1000, 2000, 4000, 8000, 16000} {
		qps = scaleQPS(qps)
		r := h.run(scenario{
			name:     fmt.Sprintf("SaturationUDP/qps=%.0f", qps),
			loads:    []load{{Name: "ramp", Proto: protoUDP, QPS: qps, Timeout: time.Second}},
			duration: step,
		})
		logReport(t, r, false)
		s := r.Loads[0].stats
		fmt.Fprintf(&summary, "%11.0f  %9.2f  %v\n", qps, 100*s.answeredFraction(), round(s.Latency.quantile(0.99)))
	}
	t.Log("\n" + summary.String())
}

// testChurnTeardown repeatedly serves a sandbox and tears it down while it is
// under UDP and TCP load, then checks that nothing leaked and that the
// relay's slots were all returned.
func testChurnTeardown(t *testing.T) {
	h := newHarness(t, behavior{delay: 20 * time.Millisecond})
	baseline := settledResources()
	deadline := time.Now().Add(*stressDuration)
	iterations := 0
	for time.Now().Before(deadline) {
		sb := h.addSandbox()
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		var wg sync.WaitGroup
		for _, l := range []load{
			{Proto: protoUDP, QPS: scaleQPS(500), Timeout: 500 * time.Millisecond},
			{Proto: protoTCP, Concurrency: 4, Timeout: 500 * time.Millisecond},
		} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sb.runLoad(ctx, l)
			}()
		}
		time.Sleep(150 * time.Millisecond)
		if err := sb.close(); err != nil {
			t.Fatalf("iteration %d: %v", iterations, err)
		}
		wg.Wait()
		cancel()
		iterations++
	}

	// A leaked slot would leave fewer than 64 for a burst of 64 queries that
	// all wait on a slow upstream at once. (A closed loop at 64 can race the
	// relay, which frees a slot only after writing the answer.)
	h.upstreams[0].setBehavior(behavior{delay: 200 * time.Millisecond})
	h.upstreams[0].resetPeak()
	sb := h.addSandbox()
	var answered atomic.Int64
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := &client{load: load{Proto: protoUDP, Timeout: 2 * time.Second}, ep: sb.endpoint, rec: newLoadRecorder()}
			q := buildQuery(uint16(i), "example.com", typeA)
			conn, err := net.Dial("udp", sb.endpoint.udp)
			if err != nil {
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			if c.exchangeUDP(conn, q, time.Now()) {
				answered.Add(1)
			}
		}()
	}
	wg.Wait()
	if err := sb.close(); err != nil {
		t.Error(err)
	}
	if n := answered.Load(); n != 64 {
		t.Errorf("after %d churn iterations, a burst of 64 queries got %d answers (upstream peak in flight %d): relay slots were not returned",
			iterations, n, h.upstreams[0].stats().PeakInFlight)
	}
	if after, ok := waitForBaseline(baseline, 5*time.Second); !ok {
		t.Errorf("after %d churn iterations: goroutines %d -> %d, fds %d -> %d",
			iterations, baseline.Goroutines, after.Goroutines, baseline.FDs, after.FDs)
	}
	t.Logf("%d serve/teardown iterations under load; all 64 relay UDP slots free afterwards", iterations)
}

// testSoak runs mixed UDP and TCP load for -stress.soak while the first
// upstream cycles through faults, to expose slow leaks and drift.
func testSoak(t *testing.T) {
	if *stressSoak <= 0 {
		t.Skip("soak; run with -stress -stress.soak=10m (and a larger -timeout)")
	}
	faults := []behavior{
		{},
		{rcode: rcodeServFail},
		{dropFraction: 0.5},
		{delay: 100 * time.Millisecond, jitter: 100 * time.Millisecond},
		{garbage: true},
		{tcpCloseOnAccept: true},
	}
	period := *stressSoak / 20
	var phases []phase
	for i := 1; i < 20; i++ {
		phases = append(phases, phase{at: time.Duration(i) * period, upstream: 0, behavior: faults[i%len(faults)]})
	}
	s := scenario{
		name:      "Soak",
		upstreams: []behavior{{}, {delay: 20 * time.Millisecond}},
		sandboxes: 4,
		loads: []load{
			{Name: "udp-0", Sandbox: 0, Proto: protoUDP, QPS: scaleQPS(200)},
			{Name: "udp-1", Sandbox: 1, Proto: protoUDP, QPS: scaleQPS(50), QTypes: []uint16{typeA, typeAAAA}},
			{Name: "tcp-2", Sandbox: 2, Proto: protoTCP, Concurrency: min(scale(4), 8), ReuseConn: true, Pipeline: true},
			{Name: "tcp-3", Sandbox: 3, Proto: protoTCP, QPS: scaleQPS(10)},
		},
		duration: *stressSoak,
		phases:   phases,
	}
	// Garbage answers are relayed verbatim, so malformed answers are expected.
	logReport(t, run(t, s), true)
}
