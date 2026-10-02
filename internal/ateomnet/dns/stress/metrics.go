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
	"math/bits"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// subBuckets is the number of linear buckets per power of two of
// microseconds, which bounds a quantile's error to 1/subBuckets (12.5%).
const subBuckets = 8

// numBuckets covers 0 to 2^40 µs (about 12 days).
const numBuckets = 41 * subBuckets

// histogram records latencies in log-linear buckets. It is safe for
// concurrent use without locking.
type histogram struct {
	buckets [numBuckets]atomic.Int64
	samples atomic.Int64
	sumUS   atomic.Int64 // µs
	maxUS   atomic.Int64 // µs
}

// record adds one latency.
func (h *histogram) record(d time.Duration) {
	us := max(d.Microseconds(), 0)
	h.buckets[bucketOf(us)].Add(1)
	h.samples.Add(1)
	h.sumUS.Add(us)
	for {
		m := h.maxUS.Load()
		if us <= m || h.maxUS.CompareAndSwap(m, us) {
			break
		}
	}
}

func bucketOf(us int64) int {
	if us < subBuckets {
		return int(us)
	}
	exp := bits.Len64(uint64(us)) - 1 // us is in [2^exp, 2^(exp+1))
	shift := exp - 3                  // log2(subBuckets)
	sub := int(us>>shift) - subBuckets
	return min((exp-2)*subBuckets+sub, numBuckets-1)
}

// bucketUpper returns the largest latency in bucket i, in µs.
func bucketUpper(i int) int64 {
	if i < subBuckets {
		return int64(i)
	}
	exp := i/subBuckets + 2
	sub := int64(i % subBuckets)
	shift := exp - 3
	return (subBuckets+sub+1)<<shift - 1
}

// count is the number of latencies recorded.
func (h *histogram) count() int64 { return h.samples.Load() }

// mean is the mean latency recorded.
func (h *histogram) mean() time.Duration {
	n := h.samples.Load()
	if n == 0 {
		return 0
	}
	return time.Duration(h.sumUS.Load()/n) * time.Microsecond
}

// max is the largest latency recorded.
func (h *histogram) max() time.Duration { return time.Duration(h.maxUS.Load()) * time.Microsecond }

// quantile returns an upper bound on the q-quantile (0 < q <= 1), accurate to
// one bucket.
func (h *histogram) quantile(q float64) time.Duration {
	n := h.samples.Load()
	if n == 0 {
		return 0
	}
	rank := int64(q*float64(n) + 0.5)
	rank = min(max(rank, 1), n)
	var seen int64
	for i := range h.buckets {
		seen += h.buckets[i].Load()
		if seen >= rank {
			return time.Duration(min(bucketUpper(i), h.maxUS.Load())) * time.Microsecond
		}
	}
	return h.max()
}

// resources is a snapshot of the process's resource use.
type resources struct {
	Goroutines int
	FDs        int // -1 where open descriptors cannot be counted
	HeapInuse  uint64
}

// sampleResources measures the process now.
func sampleResources() resources {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return resources{Goroutines: runtime.NumGoroutine(), FDs: countFDs(), HeapInuse: ms.HeapInuse}
}

// countFDs counts open descriptors other than pipes. On Linux, io.Copy between
// TCP connections (the relay's TCP path) splices through pipes that the
// runtime caches and frees lazily, so they are not the relay's to return.
func countFDs() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	n := 0
	for _, e := range entries {
		target, err := os.Readlink("/proc/self/fd/" + e.Name())
		if err != nil || strings.HasPrefix(target, "pipe:") {
			continue // includes the descriptor ReadDir held open
		}
		n++
	}
	return n
}

// sampler records the peak of each resource while it runs.
type sampler struct {
	mu     sync.Mutex
	peak   resources
	cancel context.CancelFunc
	done   chan struct{}
}

func startSampler(interval time.Duration) *sampler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &sampler{cancel: cancel, done: make(chan struct{})}
	s.observe(sampleResources())
	go func() {
		defer close(s.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.observe(sampleResources())
			}
		}
	}()
	return s
}

func (s *sampler) observe(r resources) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peak.Goroutines = max(s.peak.Goroutines, r.Goroutines)
	s.peak.FDs = max(s.peak.FDs, r.FDs)
	s.peak.HeapInuse = max(s.peak.HeapInuse, r.HeapInuse)
}

// stop ends sampling and returns the peaks.
func (s *sampler) stop() resources {
	s.cancel()
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

// waitForBaseline waits up to grace for goroutines and descriptors to fall
// back to baseline, and returns the last sample. Runtime goroutines come and
// go, so slack of a few is allowed.
func waitForBaseline(baseline resources, grace time.Duration) (resources, bool) {
	const slack = 2
	runtime.GC()
	deadline := time.Now().Add(grace)
	for {
		now := sampleResources()
		ok := now.Goroutines <= baseline.Goroutines+slack &&
			(baseline.FDs < 0 || now.FDs <= baseline.FDs)
		if ok || time.Now().After(deadline) {
			return now, ok
		}
		time.Sleep(20 * time.Millisecond)
	}
}
