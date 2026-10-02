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
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"
	"time"
)

// report is the outcome of one [scenario].
type report struct {
	Name        string
	Duration    time.Duration
	Elapsed     time.Duration // including waiting for the last answers
	Sandboxes   int
	RaceEnabled bool
	Loads       []loadReport
	Upstreams   []upstreamStats
	Baseline    resources // before the sandboxes were served
	Peak        resources // while the loads ran
	After       resources // after teardown
	// Findings are the thresholds the scenario missed.
	Findings []string
}

// loadReport pairs a load with what it observed.
type loadReport struct {
	load  load
	stats loadStats
}

// load returns the report for the load named name, or nil.
func (r *report) load(name string) *loadReport {
	for i := range r.Loads {
		if r.Loads[i].load.Name == name {
			return &r.Loads[i]
		}
	}
	return nil
}

// String renders the report as aligned text tables.
func (r *report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "=== %s: %v, %d sandbox(es)", r.Name, r.Duration, r.Sandboxes)
	if r.RaceEnabled {
		b.WriteString(", race detector on (throughput is not representative)")
	}
	b.WriteString("\n")

	tw := tabwriter.NewWriter(&b, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "load\tproto\tsent\tanswered\tok%\ttimeout\tskipped\tmalformed\tqps\tp50\tp90\tp99\tmax\t")
	for _, lr := range r.Loads {
		s := lr.stats
		window := r.Duration
		if lr.load.StopAfter > 0 && lr.load.StopAfter < window {
			window = lr.load.StopAfter
		}
		window -= lr.load.StartAfter
		qps := float64(s.Answered) / max(window.Seconds(), 1e-9)
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%.2f\t%d\t%d\t%d\t%.0f\t%v\t%v\t%v\t%v\t\n",
			lr.load.Name, lr.load.Proto, s.Sent, s.Answered, 100*s.answeredFraction(),
			s.Timeouts, s.Skipped, s.Malformed, qps,
			round(s.Latency.quantile(0.5)), round(s.Latency.quantile(0.9)),
			round(s.Latency.quantile(0.99)), round(s.Latency.max()))
	}
	_ = tw.Flush()

	for _, lr := range r.Loads {
		var extra []string
		if len(lr.stats.Rcodes) > 0 {
			extra = append(extra, "rcodes "+formatCounts(lr.stats.Rcodes))
		}
		if len(lr.stats.Errors) > 0 {
			extra = append(extra, "errors "+formatCounts(lr.stats.Errors))
		}
		if lr.load.IdleConns > 0 {
			extra = append(extra, fmt.Sprintf("idle conns held %d", lr.stats.IdleHeld))
		}
		if len(extra) > 0 {
			fmt.Fprintf(&b, "  %s: %s\n", lr.load.Name, strings.Join(extra, "; "))
		}
	}
	for i, u := range r.Upstreams {
		fmt.Fprintf(&b, "  upstream %d: queries %d, answered %d, dropped %d, tcp conns %d, peak in flight %d\n",
			i, u.Queries, u.Answered, u.Dropped, u.TCPConns, u.PeakInFlight)
	}
	fmt.Fprintf(&b, "  resources: goroutines %d/%d/%d, fds %d/%d/%d, heap %s/%s/%s (baseline/peak/after)\n",
		r.Baseline.Goroutines, r.Peak.Goroutines, r.After.Goroutines,
		r.Baseline.FDs, r.Peak.FDs, r.After.FDs,
		mib(r.Baseline.HeapInuse), mib(r.Peak.HeapInuse), mib(r.After.HeapInuse))
	for _, f := range r.Findings {
		fmt.Fprintf(&b, "  FINDING (report-only): %s\n", f)
	}
	return b.String()
}

// MarshalJSON flattens latency histograms into quantiles.
func (lr loadReport) MarshalJSON() ([]byte, error) {
	h := lr.stats.Latency
	type stats loadStats
	return json.Marshal(struct {
		Name      string
		Proto     string
		Load      load
		Stats     stats
		LatencyUS map[string]int64
	}{
		Name:  lr.load.Name,
		Proto: lr.load.Proto.String(),
		Load:  lr.load,
		Stats: stats(lr.stats),
		LatencyUS: map[string]int64{
			"mean": h.mean().Microseconds(),
			"p50":  h.quantile(0.5).Microseconds(),
			"p90":  h.quantile(0.9).Microseconds(),
			"p99":  h.quantile(0.99).Microseconds(),
			"p999": h.quantile(0.999).Microseconds(),
			"max":  h.max().Microseconds(),
		},
	})
}

// MarshalJSON omits the histogram buckets.
func (h *histogram) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]int64{"count": h.count()})
}

func formatCounts(m map[string]int64) string {
	var parts []string
	for _, k := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, " ")
}

func round(d time.Duration) time.Duration {
	switch {
	case d >= time.Second:
		return d.Round(10 * time.Millisecond)
	case d >= time.Millisecond:
		return d.Round(10 * time.Microsecond)
	}
	return d
}

func mib(b uint64) string { return fmt.Sprintf("%.1fMiB", float64(b)/(1<<20)) }
