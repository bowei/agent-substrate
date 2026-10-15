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
	"strings"
	"time"
)

// ring is a fixed-capacity buffer that overwrites its oldest line.
type ring struct {
	buf   []Line
	start int
	n     int
}

func newRing(capacity int) *ring { return &ring{buf: make([]Line, capacity)} }

func (r *ring) add(l Line) {
	if len(r.buf) == 0 {
		return
	}
	if r.n < len(r.buf) {
		r.buf[(r.start+r.n)%len(r.buf)] = l
		r.n++
		return
	}
	r.buf[r.start] = l
	r.start = (r.start + 1) % len(r.buf)
}

// each visits the lines oldest first.
func (r *ring) each(fn func(Line)) {
	for i := range r.n {
		fn(r.buf[(r.start+i)%len(r.buf)])
	}
}

// Filter selects lines. The zero Filter matches everything.
type Filter struct {
	// Sources restricts to these containers when non-nil.
	Sources map[Source]bool
	// Pods restricts to these "namespace/pod" keys (any container) when
	// non-nil. A line passes if either Sources or Pods admits it.
	Pods map[string]bool
	// MinLevel drops lines ranked below it (see LevelRank).
	MinLevel string
	// Contains is a case-insensitive substring of the raw line.
	Contains string
	// Since and Until bound the line time when non-zero.
	Since, Until time.Time
	// Match is an extra predicate when non-nil.
	Match func(Line) bool
}

// PodKey is the Filter.Pods key for a pod.
func PodKey(namespace, pod string) string { return namespace + "/" + pod }

func (f *Filter) wantsSource(src Source) bool {
	if f.Sources == nil && f.Pods == nil {
		return true
	}
	return f.Sources[src] || f.Pods[PodKey(src.Namespace, src.Pod)]
}

// Matches reports whether l passes the filter.
func (f *Filter) Matches(l Line) bool {
	if !f.wantsSource(l.Source) {
		return false
	}
	if f.MinLevel != "" && LevelRank(l.Level) < LevelRank(f.MinLevel) {
		return false
	}
	if !f.Since.IsZero() && l.Time.Before(f.Since) {
		return false
	}
	if !f.Until.IsZero() && l.Time.After(f.Until) {
		return false
	}
	if f.Contains != "" && !strings.Contains(strings.ToLower(l.Raw), strings.ToLower(f.Contains)) {
		return false
	}
	if f.Match != nil && !f.Match(l) {
		return false
	}
	return true
}
