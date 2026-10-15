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

package logs

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

var src = Source{Namespace: "ate-system", Pod: "ate-controller-1", Container: "manager"}

func TestParseLineJSON(t *testing.T) {
	text := `2026-10-15T17:43:56.800000000Z {"time":"2026-10-15T17:43:56.730049531Z","level":"warn","msg":"Syncer: x","ate.atespace":"demo","ate.actor.version":3,"pod":"ns/p"}`
	l := parseLine(src, text)
	if l.Level != "WARN" || l.Msg != "Syncer: x" {
		t.Errorf("level/msg = %q/%q", l.Level, l.Msg)
	}
	if want := time.Date(2026, 10, 15, 17, 43, 56, 730049531, time.UTC); !l.Time.Equal(want) {
		t.Errorf("Time = %v, want app time %v", l.Time, want)
	}
	if want := time.Date(2026, 10, 15, 17, 43, 56, 800000000, time.UTC); !l.kubeTime.Equal(want) {
		t.Errorf("kubeTime = %v, want %v", l.kubeTime, want)
	}
	if l.Attrs["ate.atespace"] != "demo" || l.Attrs["ate.actor.version"] != "3" {
		t.Errorf("Attrs = %v", l.Attrs)
	}
	if _, ok := l.Attrs["pod"]; ok {
		t.Errorf("non-ate attribute kept: %v", l.Attrs)
	}
	if !strings.HasPrefix(l.Raw, "{") {
		t.Errorf("Raw kept the kubelet timestamp: %q", l.Raw)
	}
}

func TestParseLinePlain(t *testing.T) {
	for _, tc := range []struct{ text, level string }{
		{"2026-10-15T17:43:56Z E1015 17:43:56.1 1 main.go:1] boom", "ERROR"},
		{"2026-10-15T17:43:56Z time=x level=warn msg=y", "WARN"},
		{"no timestamp at all", ""},
	} {
		if got := parseLine(src, tc.text).Level; got != tc.level {
			t.Errorf("parseLine(%q).Level = %q, want %q", tc.text, got, tc.level)
		}
	}
}

func TestRingOverwritesOldest(t *testing.T) {
	r := newRing(3)
	for i := range 5 {
		r.add(Line{Seq: uint64(i)})
	}
	var got []uint64
	r.each(func(l Line) { got = append(got, l.Seq) })
	if want := []uint64{2, 3, 4}; !equalU64(got, want) {
		t.Errorf("ring = %v, want %v", got, want)
	}
}

func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFilter(t *testing.T) {
	other := Source{Namespace: "ns", Pod: "w", Container: "c"}
	info := Line{Source: src, Level: "INFO", Raw: "hello World"}
	errLine := Line{Source: other, Level: "ERROR", Raw: "bad"}
	for _, tc := range []struct {
		name string
		f    Filter
		l    Line
		want bool
	}{
		{"zero matches", Filter{}, info, true},
		{"level drops", Filter{MinLevel: "WARN"}, info, false},
		{"level keeps", Filter{MinLevel: "WARN"}, errLine, true},
		{"contains is case-insensitive", Filter{Contains: "world"}, info, true},
		{"contains misses", Filter{Contains: "nope"}, info, false},
		{"pods admits", Filter{Pods: map[string]bool{"ns/w": true}}, errLine, true},
		{"pods rejects", Filter{Pods: map[string]bool{"ns/w": true}}, info, false},
		{"sources admits", Filter{Sources: map[Source]bool{src: true}}, info, true},
		{"match predicate", Filter{Match: func(Line) bool { return false }}, info, false},
	} {
		if got := tc.f.Matches(tc.l); got != tc.want {
			t.Errorf("%s: Matches = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// fakeOpener serves canned streams and records the options it was given.
type fakeOpener struct {
	mu     sync.Mutex
	bodies []string
	opts   []*corev1.PodLogOptions
}

func (f *fakeOpener) open(_ context.Context, _ Source, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opts = append(f.opts, opts)
	if len(f.bodies) == 0 {
		return nil, errors.New("no more streams")
	}
	b := f.bodies[0]
	f.bodies = f.bodies[1:]
	return io.NopCloser(strings.NewReader(b)), nil
}

func TestManagerFollowAndResume(t *testing.T) {
	fo := &fakeOpener{bodies: []string{
		"2026-10-15T10:00:00Z one\n2026-10-15T10:00:01Z two\n",
		// The reconnect overlaps the first stream by one line.
		"2026-10-15T10:00:01Z two\n2026-10-15T10:00:02Z three\n",
	}}
	m := NewManager(fo.open, Options{})
	ch, cancel := m.Subscribe(Filter{}, 10)
	defer cancel()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	m.Follow(ctx, src)

	var got []string
	timeout := time.After(10 * time.Second)
	for len(got) < 3 {
		select {
		case l := <-ch:
			got = append(got, l.Raw)
		case <-timeout:
			t.Fatalf("timed out; got %v", got)
		}
	}
	if strings.Join(got, ",") != "one,two,three" {
		t.Errorf("lines = %v", got)
	}
	m.Close()

	fo.mu.Lock()
	defer fo.mu.Unlock()
	if fo.opts[0].TailLines == nil || fo.opts[0].SinceTime != nil {
		t.Errorf("first open should tail: %+v", fo.opts[0])
	}
	if fo.opts[1].SinceTime == nil || fo.opts[1].TailLines != nil {
		t.Errorf("reconnect should resume with SinceTime: %+v", fo.opts[1])
	}
	if q := m.Query(Filter{Contains: "t"}, 0); len(q) != 2 {
		t.Errorf("Query = %v, want two and three", q)
	}
}

func TestManagerSyncStopsAndGC(t *testing.T) {
	fo := &fakeOpener{}
	m := NewManager(fo.open, Options{Retain: time.Minute})
	now := time.Now()
	m.now = func() time.Time { return now }
	ctx := context.Background()
	pinned := Source{Namespace: "ns", Pod: "w", Container: "c"}

	m.Sync(ctx, map[Source]bool{src: true})
	m.Touch(ctx, pinned)
	m.Sync(ctx, map[Source]bool{})
	info := map[Source]StreamInfo{}
	for _, s := range m.Streams() {
		info[s.Source] = s
	}
	if info[src].Following {
		t.Errorf("unwanted stream still following")
	}
	if !info[pinned].Following || !info[pinned].Pinned {
		t.Errorf("pinned stream stopped: %+v", info[pinned])
	}

	now = now.Add(2 * time.Minute)
	m.GC()
	if got := len(m.Streams()); got != 1 {
		t.Errorf("after GC %d streams, want only the pinned one", got)
	}
	m.Close()
}

func TestQueryLimitAndOrder(t *testing.T) {
	m := NewManager((&fakeOpener{}).open, Options{})
	a := Source{Namespace: "n", Pod: "a", Container: "c"}
	b := Source{Namespace: "n", Pod: "b", Container: "c"}
	m.Append(a, "2026-10-15T10:00:02Z a2")
	m.Append(b, "2026-10-15T10:00:01Z b1")
	m.Append(a, "2026-10-15T10:00:03Z a3")
	got := m.Query(Filter{}, 2)
	if len(got) != 2 || got[0].Raw != "a2" || got[1].Raw != "a3" {
		t.Errorf("Query = %+v", got)
	}
}
