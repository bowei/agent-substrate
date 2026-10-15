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

package monitor

import (
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/e2efeed"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
)

const (
	// LaneTest holds the test's own output.
	LaneTest = "test"
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

// TimelineOptions tunes Timeline.
type TimelineOptions struct {
	// Own correlates only the resources the test tracked itself, not the
	// ones it shares with its parent and siblings (such as the fixture
	// namespace parallel subtests all use).
	Own bool
	// Limit caps the log lines; 0 means 3000.
	Limit int
}

// refsFor returns a test's tracked resources (its own, its ancestors', and
// its descendants') plus the namespaces annotated with its name or a
// descendant's.
func (m *Monitor) refsFor(t e2efeed.Test) []e2etrack.Ref {
	refs := m.cfg.Feed.RefsFor(t.ID())
	for _, ns := range m.cfg.Discovery.Namespaces() {
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

// laneMap maps "namespace/pod" to its component, or "workers/<pool>" for a
// worker pod.
func (m *Monitor) laneMap() map[string]string {
	lanes := map[string]string{}
	for _, c := range m.cfg.Discovery.Components() {
		for _, p := range c.Pods {
			lanes[logs.PodKey(p.Namespace, p.Name)] = c.Name
		}
	}
	for _, pool := range m.cfg.Discovery.Pools() {
		for _, p := range pool.Pods {
			lanes[logs.PodKey(p.Namespace, p.Name)] = "workers/" + pool.Name
		}
	}
	return lanes
}

// Timeline builds the timeline of a test.
func (m *Monitor) Timeline(id string, opts TimelineOptions) (Timeline, bool) {
	t, ok := m.cfg.Feed.Test(id)
	if !ok {
		return Timeline{}, false
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 3000
	}
	tl := Timeline{Test: t, Refs: m.refsFor(t)}
	if opts.Own && len(t.Refs) > 0 {
		tl.Refs = t.Refs
		tl.Scope = "own"
	}

	// The test's own output and its subtests'.
	for _, st := range m.cfg.Feed.Summary().Tests {
		if st.Package != t.Package || (st.Name != t.Name && !strings.HasPrefix(st.Name, t.Name+"/")) {
			continue
		}
		full, ok := m.cfg.Feed.Test(st.ID())
		if !ok {
			continue
		}
		for _, o := range full.Output {
			if _, isMarker := e2etrack.Parse(o.Text); isMarker {
				continue
			}
			tl.Entries = append(tl.Entries, TimelineEntry{Time: o.Time, Lane: LaneTest, Kind: "test", Test: full.Name, Text: o.Text})
		}
	}

	matcher := e2efeed.NewMatcher(tl.Refs)
	if !matcher.Empty() {
		f := logs.Filter{Match: matcher.Match, Since: t.Start.Add(-timelineLead)}
		if !t.End.IsZero() {
			f.Until = t.End.Add(timelineTail)
		}
		lines := m.cfg.Logs.Query(f, limit)
		tl.Truncated = len(lines) == limit
		lanes := m.laneMap()
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
	seen := map[string]bool{LaneTest: true}
	tl.Lanes = []string{LaneTest}
	for _, e := range tl.Entries {
		if !seen[e.Lane] {
			seen[e.Lane] = true
			tl.Lanes = append(tl.Lanes, e.Lane)
		}
	}
	return tl, true
}
