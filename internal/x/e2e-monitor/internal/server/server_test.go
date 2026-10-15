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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/discovery"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/monitor"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/monitor/monitortest"
)

func newServer(t *testing.T) (*monitortest.Fixture, *httptest.Server) {
	t.Helper()
	f := monitortest.New(t, monitor.FollowE2E)
	hs := httptest.NewServer(New(f.Monitor).Handler())
	t.Cleanup(hs.Close)
	return f, hs
}

func get(t *testing.T, hs *httptest.Server, path string, want int, v any) {
	t.Helper()
	resp, err := http.Get(hs.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: %d %s, want %d", path, resp.StatusCode, b, want)
	}
	if v != nil {
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			t.Fatalf("GET %s: decoding: %v", path, err)
		}
	}
}

func TestEndpoints(t *testing.T) {
	f, hs := newServer(t)
	var cs []discovery.Component
	get(t, hs, "/api/components", http.StatusOK, &cs)
	if len(cs) != 1 || cs[0].Name != "ate-controller" {
		t.Errorf("components = %+v", cs)
	}
	get(t, hs, "/api/components/ate-controller", http.StatusOK, nil)
	get(t, hs, "/api/components/nope", http.StatusNotFound, nil)
	var pools []discovery.Pool
	get(t, hs, "/api/pools", http.StatusOK, &pools)
	if len(pools) != 2 {
		t.Errorf("pools = %+v", pools)
	}
	var info monitor.Info
	get(t, hs, "/api/info", http.StatusOK, &info)
	if info.FollowWorkers != monitor.FollowE2E {
		t.Errorf("info = %+v", info)
	}
	get(t, hs, "/api/substrate", http.StatusOK, nil)
	get(t, hs, "/api/streams", http.StatusOK, nil)
	get(t, hs, "/api/namespaces", http.StatusOK, nil)

	f.Logs.Append(monitortest.Controller, `2026-10-15T10:00:00Z {"level":"ERROR","msg":"boom"}`)
	var lines []logs.Line
	get(t, hs, "/api/logs?component=ate-controller&level=ERROR", http.StatusOK, &lines)
	if len(lines) != 1 || lines[0].Msg != "boom" {
		t.Errorf("lines = %+v", lines)
	}
	get(t, hs, "/api/logs?component=nope", http.StatusBadRequest, nil)
	get(t, hs, "/api/logs?since=yesterday", http.StatusBadRequest, nil)
	get(t, hs, "/api/logs/previous?pod=x", http.StatusBadRequest, nil)

	start := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	f.StartTest(start, e2etrack.Ref{Kind: e2etrack.KindNamespace, Name: monitortest.TestNamespace})
	id := url.QueryEscape(monitortest.TestID)
	var detail monitor.TestDetail
	get(t, hs, "/api/e2e/test?id="+id, http.StatusOK, &detail)
	if detail.Name != "TestA" || len(detail.AllRefs) != 1 {
		t.Errorf("test = %+v", detail)
	}
	var tl monitor.Timeline
	get(t, hs, "/api/e2e/timeline?scope=own&id="+id, http.StatusOK, &tl)
	if tl.Scope != "own" || len(tl.Entries) == 0 {
		t.Errorf("timeline = %+v", tl)
	}
	get(t, hs, "/api/e2e/test?id=nope", http.StatusNotFound, nil)
	get(t, hs, "/api/e2e/package?name=p", http.StatusOK, nil)
	get(t, hs, "/api/e2e", http.StatusOK, nil)

	for _, path := range []string{"/", "/app.js", "/style.css"} {
		get(t, hs, path, http.StatusOK, nil)
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

func stream(t *testing.T, hs *httptest.Server, path string) <-chan sseEvent {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, hs.URL+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return readSSE(resp.Body)
}

func next(t *testing.T, events <-chan sseEvent) sseEvent {
	t.Helper()
	select {
	case ev := <-events:
		return ev
	case <-time.After(10 * time.Second):
		t.Fatal("no event")
		return sseEvent{}
	}
}

func TestLogStream(t *testing.T) {
	f, hs := newServer(t)
	f.Logs.Append(monitortest.Controller, `2026-10-15T10:00:00Z old`)
	events := stream(t, hs, "/api/logs/stream?component=ate-controller")
	if ev := next(t, events); ev.name != "backlog" || !strings.Contains(ev.data, `"raw":"old"`) {
		t.Fatalf("first event = %+v", ev)
	}
	f.Logs.Append(monitortest.Controller, `2026-10-15T10:00:01Z new`)
	if ev := next(t, events); ev.name != "lines" || !strings.Contains(ev.data, `"raw":"new"`) || strings.Contains(ev.data, `"raw":"old"`) {
		t.Errorf("second event = %+v", ev)
	}
}

func TestEvents(t *testing.T) {
	f, hs := newServer(t)
	events := stream(t, hs, "/api/events")
	// Give the handler a moment to subscribe.
	time.Sleep(100 * time.Millisecond)
	f.StartTest(time.Now())
	if ev := next(t, events); ev.name != string(monitor.TopicE2E) {
		t.Errorf("event = %+v", ev)
	}
}
