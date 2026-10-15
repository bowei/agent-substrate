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

package monitor_test

import (
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/monitor"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/monitor/monitortest"
)

var nsRef = e2etrack.Ref{Kind: e2etrack.KindNamespace, Name: monitortest.TestNamespace}

func TestLogs(t *testing.T) {
	f := monitortest.New(t, monitor.FollowE2E)
	m := f.Monitor
	f.Logs.Append(monitortest.Controller, `2026-10-15T10:00:00Z {"level":"INFO","msg":"hello"}`)
	f.Logs.Append(monitortest.Controller, `2026-10-15T10:00:01Z {"level":"ERROR","msg":"boom"}`)
	f.Logs.Append(logs.Source{Namespace: "x", Pod: "y", Container: "z"}, `2026-10-15T10:00:02Z elsewhere`)

	for _, tc := range []struct {
		q    monitor.LogQuery
		want int
	}{
		{monitor.LogQuery{Component: "ate-controller"}, 2},
		{monitor.LogQuery{Component: "ate-controller", MinLevel: "ERROR"}, 1},
		{monitor.LogQuery{Pod: "ate-system/ate-controller-1", Contains: "hello"}, 1},
		{monitor.LogQuery{Pod: "ate-system/ate-controller-1", Container: "main", Limit: 1}, 1},
		{monitor.LogQuery{}, 3},
	} {
		got, err := m.Logs(tc.q)
		if err != nil || len(got) != tc.want {
			t.Errorf("Logs(%+v) = %d lines, %v; want %d", tc.q, len(got), err, tc.want)
		}
	}
	if _, err := m.Logs(monitor.LogQuery{Component: "nope"}); err == nil {
		t.Error("unknown component accepted")
	}
	if _, err := m.Logs(monitor.LogQuery{Pod: "no-slash"}); err == nil {
		t.Error("malformed pod accepted")
	}
	// Asking for a pod follows it on demand.
	pinned := false
	for _, s := range m.Streams() {
		if s.Source == monitortest.Controller && s.Pinned {
			pinned = true
		}
	}
	if !pinned {
		t.Errorf("pod not followed on demand: %+v", m.Streams())
	}
}

func TestSubscribe(t *testing.T) {
	f := monitortest.New(t, monitor.FollowE2E)
	f.Logs.Append(monitortest.Controller, `2026-10-15T10:00:00Z old`)
	sub, err := f.Monitor.Subscribe(monitor.LogQuery{Component: "ate-controller"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Cancel()
	if len(sub.Backlog) != 1 || sub.LastSeq != sub.Backlog[0].Seq {
		t.Fatalf("backlog = %+v, lastSeq %d", sub.Backlog, sub.LastSeq)
	}
	f.Logs.Append(monitortest.Controller, `2026-10-15T10:00:01Z new`)
	select {
	case l := <-sub.C:
		if l.Raw != "new" || l.Seq <= sub.LastSeq {
			t.Errorf("live line = %+v", l)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no live line")
	}
}

func TestChanges(t *testing.T) {
	f := monitortest.New(t, monitor.FollowE2E)
	ch, stop := f.Monitor.Changes()
	defer stop()
	f.StartTest(time.Now())
	select {
	case topic := <-ch:
		if topic != monitor.TopicE2E {
			t.Errorf("topic = %q", topic)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no change notification")
	}
	stop() // Idempotent.
}

func TestTimeline(t *testing.T) {
	f := monitortest.New(t, monitor.FollowE2E)
	start := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	f.StartTest(start, nsRef)
	f.EndTest(start.Add(3*time.Second), "fail")

	f.Logs.Append(monitortest.Controller, `2026-10-15T10:00:01Z {"level":"INFO","msg":"Syncer: registering worker","pod":"abcd-123/counter-1"}`)
	f.Logs.Append(monitortest.Controller, `2026-10-15T10:00:01Z {"level":"INFO","msg":"unrelated","pod":"zzzz-999/counter-2"}`)
	// Worker lines match by the pod's namespace, whatever they say.
	f.Logs.Append(monitortest.Worker1, `2026-10-15T10:00:02Z {"level":"ERROR","msg":"restore failed"}`)
	f.Logs.Append(monitortest.Controller, `2026-10-15T11:00:00Z {"level":"INFO","msg":"long after, abcd-123/counter-1"}`)

	tl, ok := f.Monitor.Timeline(monitortest.TestID, monitor.TimelineOptions{})
	if !ok {
		t.Fatal("no timeline")
	}
	var texts []string
	for _, e := range tl.Entries {
		texts = append(texts, e.Lane+": "+e.Text)
	}
	want := []string{
		"test:     a_test.go:10: starting",
		"ate-controller: Syncer: registering worker",
		"workers/counter: restore failed",
	}
	if strings.Join(texts, "\n") != strings.Join(want, "\n") {
		t.Errorf("timeline:\n%s\nwant:\n%s", strings.Join(texts, "\n"), strings.Join(want, "\n"))
	}
	if strings.Join(tl.Lanes, ",") != "test,ate-controller,workers/counter" {
		t.Errorf("lanes = %v", tl.Lanes)
	}
	if _, ok := f.Monitor.Timeline("p.Nope", monitor.TimelineOptions{}); ok {
		t.Error("timeline for an unknown test")
	}
	d, ok := f.Monitor.Test(monitortest.TestID)
	if !ok || len(d.AllRefs) != 1 {
		t.Errorf("Test = %+v", d)
	}
}

func TestDesiredSources(t *testing.T) {
	ctrl, w1, w2 := monitortest.Controller, monitortest.Worker1, monitortest.Worker2
	for _, tc := range []struct {
		mode   monitor.FollowMode
		active bool
		want   []logs.Source
	}{
		{monitor.FollowNone, true, []logs.Source{ctrl}},
		{monitor.FollowE2E, false, []logs.Source{ctrl}},
		{monitor.FollowE2E, true, []logs.Source{ctrl, w1}},
		{monitor.FollowAll, false, []logs.Source{ctrl, w1, w2}},
	} {
		f := monitortest.New(t, tc.mode)
		if tc.active {
			f.StartTest(time.Now(), nsRef)
		}
		got := f.Monitor.DesiredSources()
		if len(got) != len(tc.want) {
			t.Errorf("%s active=%v: desired = %v, want %v", tc.mode, tc.active, got, tc.want)
			continue
		}
		for _, s := range tc.want {
			if !got[s] {
				t.Errorf("%s active=%v: missing %v in %v", tc.mode, tc.active, s, got)
			}
		}
	}
}

func TestParseFollowMode(t *testing.T) {
	if _, err := monitor.ParseFollowMode("bogus"); err == nil {
		t.Error("bogus mode accepted")
	}
	if m, err := monitor.ParseFollowMode("all"); err != nil || m != monitor.FollowAll {
		t.Errorf("ParseFollowMode(all) = %v, %v", m, err)
	}
}

func TestSubstrateDisabled(t *testing.T) {
	f := monitortest.New(t, monitor.FollowE2E)
	if s := f.Monitor.Substrate(); s.APIError == "" {
		t.Errorf("Substrate() without a poller = %+v", s)
	}
}
