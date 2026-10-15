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

package server

import (
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/e2efeed"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
)

const (
	// laneTest holds the test's own output.
	laneTest = "test"
	// timelineLead and timelineTail widen the log window around the test.
	timelineLead = 2 * time.Second
	timelineTail = 10 * time.Second
)

// TimelineEntry is one row of a test's timeline.
type TimelineEntry struct {
	Time  time.Time `json:"time"`
	Lane  string    `json:"lane"`
	Kind  string    `json:"kind"`
	Test  string    `json:"test,omitempty"`
	Pod   string    `json:"pod,omitempty"`
	Level string    `json:"level,omitempty"`
	Text  string    `json:"text"`
}

// Timeline is a test's output merged with the component log lines that
// mention its resources.
type Timeline struct {
	Test    e2efeed.Test    `json:"test"`
	Refs    []e2etrack.Ref  `json:"refs"`
	Lanes   []string        `json:"lanes"`
	Entries []TimelineEntry `json:"entries"`
	// Truncated is set when the log lines were capped at the limit.
	Truncated bool `json:"truncated,omitempty"`
	// Scope is "own" when only the test's own resources were correlated.
	Scope string `json:"scope,omitempty"`
}

// refsFor returns a test's tracked resources plus the namespaces annotated
// with its name or a descendant's.
func (s *Server) refsFor(t e2efeed.Test) []e2etrack.Ref {
	refs := s.cfg.Feed.RefsFor(t.ID())
	for _, ns := range s.cfg.Discovery.Namespaces() {
		if ns.Test == "" || (ns.Test != t.Name && !strings.HasPrefix(ns.Test, t.Name+"/")) {
			continue
		}
		ref := e2etrack.Ref{Kind: e2etrack.KindNamespace, Name: ns.Name}
		if !slices.Contains(refs, ref) {
			refs = append(refs, ref)
		}
	}
	return refs
}

// laneFor maps a pod to its component, or "workers/<pool>" for a worker pod.
func (s *Server) laneMap() map[string]string {
	lanes := map[string]string{}
	for _, c := range s.cfg.Discovery.Components() {
		for _, p := range c.Pods {
			lanes[logs.PodKey(p.Namespace, p.Name)] = c.Name
		}
	}
	for _, pool := range s.cfg.Discovery.Pools() {
		for _, p := range pool.Pods {
			lanes[logs.PodKey(p.Namespace, p.Name)] = "workers/" + pool.Name
		}
	}
	return lanes
}

func (s *Server) handleTimeline(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	t, ok := s.cfg.Feed.Test(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	limit := queryInt(r, "limit", 3000)
	tl := Timeline{Test: t, Refs: s.refsFor(t)}
	// scope=own correlates only the resources this test tracked itself, not
	// the ones it shares with its parent and siblings (such as the fixture
	// namespace parallel subtests all use).
	if r.URL.Query().Get("scope") == "own" && len(t.Refs) > 0 {
		tl.Refs = t.Refs
		tl.Scope = "own"
	}

	// The test's own output and its subtests'.
	for _, st := range s.cfg.Feed.Summary().Tests {
		if st.Package != t.Package || (st.Name != t.Name && !strings.HasPrefix(st.Name, t.Name+"/")) {
			continue
		}
		full, ok := s.cfg.Feed.Test(st.ID())
		if !ok {
			continue
		}
		for _, o := range full.Output {
			if _, isMarker := e2etrack.Parse(o.Text); isMarker {
				continue
			}
			tl.Entries = append(tl.Entries, TimelineEntry{Time: o.Time, Lane: laneTest, Kind: "test", Test: full.Name, Text: o.Text})
		}
	}

	matcher := e2efeed.NewMatcher(tl.Refs)
	if !matcher.Empty() {
		f := logs.Filter{Match: matcher.Match, Since: t.Start.Add(-timelineLead)}
		if !t.End.IsZero() {
			f.Until = t.End.Add(timelineTail)
		}
		lines := s.cfg.Logs.Query(f, limit)
		tl.Truncated = len(lines) == limit
		lanes := s.laneMap()
		for _, l := range lines {
			pod := logs.PodKey(l.Namespace, l.Pod)
			lane, ok := lanes[pod]
			if !ok {
				lane = l.Namespace
			}
			text := l.Msg
			if text == "" {
				text = l.Raw
			}
			tl.Entries = append(tl.Entries, TimelineEntry{Time: l.Time, Lane: lane, Kind: "log", Pod: pod + "/" + l.Container, Level: l.Level, Text: text})
		}
	}

	sort.SliceStable(tl.Entries, func(i, j int) bool { return tl.Entries[i].Time.Before(tl.Entries[j].Time) })
	seen := map[string]bool{laneTest: true}
	tl.Lanes = []string{laneTest}
	for _, e := range tl.Entries {
		if !seen[e.Lane] {
			seen[e.Lane] = true
			tl.Lanes = append(tl.Lanes, e.Lane)
		}
	}
	writeJSON(w, tl)
}
