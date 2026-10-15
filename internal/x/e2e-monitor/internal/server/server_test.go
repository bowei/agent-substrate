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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/discovery"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/e2efeed"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
)

func runningPod(ns, name string, lbls map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: lbls},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

type fixture struct {
	srv  *Server
	logs *logs.Manager
	feed *e2efeed.Feed
	http *httptest.Server
}

func newFixture(t *testing.T, follow FollowMode) *fixture {
	t.Helper()
	one := int32(1)
	objs := []runtime.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "ate-controller"},
			Spec: appsv1.DeploymentSpec{
				Replicas: &one,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "ate-controller"}},
			},
		},
		runningPod("ate-system", "ate-controller-1", map[string]string{"app": "ate-controller"}),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "abcd-123", Labels: map[string]string{discovery.E2ENamespaceLabel: "true"}}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "zzzz-999", Labels: map[string]string{discovery.E2ENamespaceLabel: "true"}}},
		runningPod("abcd-123", "counter-1", map[string]string{discovery.WorkerPoolLabel: "counter"}),
		runningPod("zzzz-999", "counter-2", map[string]string{discovery.WorkerPoolLabel: "counter"}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	disc := discovery.New(discovery.Config{Kube: k8sfake.NewClientset(objs...), Namespaces: []string{"ate-system"}})
	if err := disc.Start(ctx, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	opener := func(ctx context.Context, _ logs.Source, _ *corev1.PodLogOptions) (io.ReadCloser, error) {
		return nil, errors.New("no streams in tests")
	}
	lm := logs.NewManager(opener, logs.Options{})
	t.Cleanup(lm.Close)
	feed := e2efeed.New()
	srv := New(ctx, Config{Discovery: disc, Logs: lm, Feed: feed, FollowWorkers: follow, Info: Info{SystemNamespace: "ate-system"}})
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	return &fixture{srv: srv, logs: lm, feed: feed, http: hs}
}

func (f *fixture) get(t *testing.T, path string, v any) {
	t.Helper()
	resp, err := http.Get(f.http.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: %d %s", path, resp.StatusCode, b)
	}
	if v != nil {
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			t.Fatalf("GET %s: decoding: %v", path, err)
		}
	}
}

func startTest(feed *e2efeed.Feed, at time.Time, refs ...e2etrack.Ref) {
	feed.Apply(e2efeed.Event{Time: at, Action: "run", Package: "p", Test: "TestA"})
	feed.Apply(e2efeed.Event{Time: at, Action: "output", Package: "p", Test: "TestA", Output: "    a_test.go:10: starting\n"})
	for _, r := range refs {
		feed.Apply(e2efeed.Event{Time: at, Action: "output", Package: "p", Test: "TestA", Output: "    a_test.go:11: " + e2etrack.Format(r) + "\n"})
	}
}

func TestComponentsAndUI(t *testing.T) {
	f := newFixture(t, FollowE2E)
	var cs []discovery.Component
	f.get(t, "/api/components", &cs)
	if len(cs) != 1 || cs[0].Name != "ate-controller" || len(cs[0].Pods) != 1 {
		t.Errorf("components = %+v", cs)
	}
	var c discovery.Component
	f.get(t, "/api/components/ate-controller", &c)
	if c.Description == "" {
		t.Errorf("component = %+v", c)
	}
	resp, err := http.Get(f.http.URL + "/api/components/nope")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown component: %d", resp.StatusCode)
	}

	for _, path := range []string{"/", "/app.js", "/style.css"} {
		resp, err := http.Get(f.http.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s: %d", path, resp.StatusCode)
		}
	}
}

func TestLogsQuery(t *testing.T) {
	f := newFixture(t, FollowE2E)
	ctrl := logs.Source{Namespace: "ate-system", Pod: "ate-controller-1", Container: "main"}
	f.logs.Append(ctrl, `2026-10-15T10:00:00Z {"level":"INFO","msg":"hello"}`)
	f.logs.Append(ctrl, `2026-10-15T10:00:01Z {"level":"ERROR","msg":"boom"}`)
	f.logs.Append(logs.Source{Namespace: "x", Pod: "y", Container: "z"}, `2026-10-15T10:00:02Z elsewhere`)

	var lines []logs.Line
	f.get(t, "/api/logs?component=ate-controller", &lines)
	if len(lines) != 2 {
		t.Errorf("component lines = %+v", lines)
	}
	f.get(t, "/api/logs?component=ate-controller&level=ERROR", &lines)
	if len(lines) != 1 || lines[0].Msg != "boom" {
		t.Errorf("error lines = %+v", lines)
	}
	f.get(t, "/api/logs?pod=ate-system/ate-controller-1&container=main&q=hello", &lines)
	if len(lines) != 1 {
		t.Errorf("pod lines = %+v", lines)
	}
	// Asking for a pod follows it on demand.
	found := false
	for _, s := range f.logs.Streams() {
		if s.Source == ctrl && s.Pinned {
			found = true
		}
	}
	if !found {
		t.Errorf("pod not pinned after a request: %+v", f.logs.Streams())
	}
}

func TestLogStreamSSE(t *testing.T) {
	f := newFixture(t, FollowE2E)
	ctrl := logs.Source{Namespace: "ate-system", Pod: "ate-controller-1", Container: "main"}
	f.logs.Append(ctrl, `2026-10-15T10:00:00Z old`)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, f.http.URL+"/api/logs/stream?component=ate-controller", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	events := readSSE(resp.Body)

	ev := <-events
	if ev.name != "backlog" || !strings.Contains(ev.data, `"raw":"old"`) {
		t.Fatalf("first event = %+v", ev)
	}
	f.logs.Append(ctrl, `2026-10-15T10:00:01Z new`)
	select {
	case ev := <-events:
		if ev.name != "lines" || !strings.Contains(ev.data, `"raw":"new"`) || strings.Contains(ev.data, `"raw":"old"`) {
			t.Errorf("second event = %+v", ev)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no live line")
	}
}

type sseEvent struct{ name, data string }

func readSSE(r io.Reader) <-chan sseEvent {
	ch := make(chan sseEvent, 16)
	go func() {
		defer close(ch)
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 1024*1024), 1024*1024)
		var ev sseEvent
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				ev.name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				ev.data = strings.TrimPrefix(line, "data: ")
			case line == "" && ev.name != "":
				ch <- ev
				ev = sseEvent{}
			}
		}
	}()
	return ch
}

func TestTimeline(t *testing.T) {
	f := newFixture(t, FollowE2E)
	start := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	startTest(f.feed, start, e2etrack.Ref{Kind: e2etrack.KindNamespace, Name: "abcd-123"})
	f.feed.Apply(e2efeed.Event{Time: start.Add(3 * time.Second), Action: "fail", Package: "p", Test: "TestA", Elapsed: 3})

	ctrl := logs.Source{Namespace: "ate-system", Pod: "ate-controller-1", Container: "main"}
	worker := logs.Source{Namespace: "abcd-123", Pod: "counter-1", Container: "main"}
	f.logs.Append(ctrl, `2026-10-15T10:00:01Z {"level":"INFO","msg":"Syncer: registering worker","pod":"abcd-123/counter-1"}`)
	f.logs.Append(ctrl, `2026-10-15T10:00:01Z {"level":"INFO","msg":"unrelated","pod":"zzzz-999/counter-2"}`)
	f.logs.Append(worker, `2026-10-15T10:00:02Z {"level":"ERROR","msg":"restore failed in abcd-123"}`)
	f.logs.Append(ctrl, `2026-10-15T11:00:00Z {"level":"INFO","msg":"long after, abcd-123/counter-1"}`)

	var tl Timeline
	f.get(t, "/api/e2e/timeline?id="+url.QueryEscape("p.TestA"), &tl)
	var texts []string
	for _, e := range tl.Entries {
		texts = append(texts, e.Lane+": "+e.Text)
	}
	want := []string{
		"test:     a_test.go:10: starting",
		"ate-controller: Syncer: registering worker",
		"workers/counter: restore failed in abcd-123",
	}
	if strings.Join(texts, "\n") != strings.Join(want, "\n") {
		t.Errorf("timeline:\n%s\nwant:\n%s", strings.Join(texts, "\n"), strings.Join(want, "\n"))
	}
	if strings.Join(tl.Lanes, ",") != "test,ate-controller,workers/counter" {
		t.Errorf("lanes = %v", tl.Lanes)
	}
}

func TestDesiredSources(t *testing.T) {
	ctrl := logs.Source{Namespace: "ate-system", Pod: "ate-controller-1", Container: "main"}
	w1 := logs.Source{Namespace: "abcd-123", Pod: "counter-1", Container: "main"}
	w2 := logs.Source{Namespace: "zzzz-999", Pod: "counter-2", Container: "main"}
	for _, tc := range []struct {
		mode   FollowMode
		active bool
		want   []logs.Source
	}{
		{FollowNone, true, []logs.Source{ctrl}},
		{FollowE2E, false, []logs.Source{ctrl}},
		{FollowE2E, true, []logs.Source{ctrl, w1}},
		{FollowAll, false, []logs.Source{ctrl, w1, w2}},
	} {
		f := newFixture(t, tc.mode)
		if tc.active {
			startTest(f.feed, time.Now(), e2etrack.Ref{Kind: e2etrack.KindNamespace, Name: "abcd-123"})
		}
		got := f.srv.desiredSources()
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
	if _, err := ParseFollowMode("bogus"); err == nil {
		t.Error("bogus mode accepted")
	}
	if m, err := ParseFollowMode("all"); err != nil || m != FollowAll {
		t.Errorf("ParseFollowMode(all) = %v, %v", m, err)
	}
}
