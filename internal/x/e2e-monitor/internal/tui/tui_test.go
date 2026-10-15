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

package tui

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/monitor"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/monitor/monitortest"
)

func newModel(t *testing.T) (*Model, *monitortest.Fixture) {
	t.Helper()
	f := monitortest.New(t, monitor.FollowE2E)
	m := New(f.Monitor)
	t.Cleanup(m.Close)
	m.Update(tea.WindowSizeMsg{Width: 160, Height: 40})
	return m, f
}

func key(s string) tea.KeyMsg {
	switch s {
	case "up":
		return tea.KeyMsg{Type: tea.KeyUp}
	case "down":
		return tea.KeyMsg{Type: tea.KeyDown}
	case "left":
		return tea.KeyMsg{Type: tea.KeyLeft}
	case "right":
		return tea.KeyMsg{Type: tea.KeyRight}
	case "enter":
		return tea.KeyMsg{Type: tea.KeyEnter}
	case "esc":
		return tea.KeyMsg{Type: tea.KeyEsc}
	}
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
}

// press sends keys and returns the last command.
func press(m *Model, keys ...string) tea.Cmd {
	var cmd tea.Cmd
	for _, k := range keys {
		_, cmd = m.Update(key(k))
	}
	return cmd
}

func screen(m *Model) string { return ansi.Strip(m.View()) }

func wantScreen(t *testing.T, m *Model, wants ...string) {
	t.Helper()
	s := screen(m)
	for _, w := range wants {
		if !strings.Contains(s, w) {
			t.Errorf("screen lacks %q:\n%s", w, s)
		}
	}
}

func dontWantScreen(t *testing.T, m *Model, nots ...string) {
	t.Helper()
	s := screen(m)
	for _, n := range nots {
		if strings.Contains(s, n) {
			t.Errorf("screen has %q:\n%s", n, s)
		}
	}
}

// runLines runs the command a log subscription returned once a line is
// available and feeds the result back.
func runLines(t *testing.T, m *Model, cmd tea.Cmd) {
	t.Helper()
	if cmd == nil {
		t.Fatal("no log command")
	}
	done := make(chan tea.Msg, 1)
	go func() { done <- cmd() }()
	select {
	case msg := <-done:
		m.Update(msg)
	case <-time.After(5 * time.Second):
		t.Fatal("no live lines")
	}
}

func TestOverview(t *testing.T) {
	m, _ := newModel(t)
	wantScreen(t, m, "Substrate e2e-monitor", "context fake", "Overview", "ate-controller", "Workers", "2 pools (2 e2e)", "E2E run", "no run attached")
	press(m, "enter")
	if m.tab().kind != tabComponent || m.tab().name != "ate-controller" {
		t.Fatalf("enter opened %+v", m.tab())
	}
	wantScreen(t, m, "all pods (1)", "ate-controller-1")
}

func TestComponentLogs(t *testing.T) {
	m, f := newModel(t)
	f.Logs.Append(monitortest.Controller, `2026-10-15T10:00:00Z {"level":"INFO","msg":"hello world","pod":"x/y"}`)
	f.Logs.Append(monitortest.Controller, `2026-10-15T10:00:01Z {"level":"ERROR","msg":"it broke"}`)
	press(m, "right")
	wantScreen(t, m, "logs", "all 1 pods", "hello world", "pod=x/y", "it broke", "following")

	// The minimum level hides INFO.
	press(m, "v", "v")
	wantScreen(t, m, "level ≥ WARN", "it broke")
	dontWantScreen(t, m, "hello world")
	press(m, "v", "v")

	// Selecting the pod resubscribes to it; live lines arrive.
	cmd := press(m, "down")
	wantScreen(t, m, "· ate-controller-1 ·")
	f.Logs.Append(monitortest.Controller, `2026-10-15T10:00:02Z {"level":"INFO","msg":"a live line"}`)
	runLines(t, m, cmd)
	wantScreen(t, m, "a live line")

	// The text filter.
	press(m, "/", "b", "r", "o", "k", "enter")
	wantScreen(t, m, `filter "brok"`, "it broke")
	dontWantScreen(t, m, "a live line")
	press(m, "esc")
	wantScreen(t, m, "a live line")

	// Events instead of logs.
	press(m, "e")
	wantScreen(t, m, "events", "No recent events.")
}

func TestWorkers(t *testing.T) {
	m, f := newModel(t)
	m.switchTab(len(m.tabs) - 2)
	wantScreen(t, m, "abcd-123/counter", "zzzz-999/counter", "Select a worker pod")
	// Down onto the first pool's pod: it is followed on demand.
	press(m, "down")
	wantScreen(t, m, "abcd-123/counter-1")
	pinned := false
	for _, s := range f.Monitor.Streams() {
		if s.Source == monitortest.Worker1 && s.Pinned {
			pinned = true
		}
	}
	if !pinned {
		t.Errorf("selected worker pod not followed: %+v", f.Monitor.Streams())
	}
	press(m, "a")
	wantScreen(t, m, "ateapi", "Registered workers (0)", "ateapi polling is disabled")
}

func TestE2E(t *testing.T) {
	m, f := newModel(t)
	press(m, "left")
	if m.tab().kind != tabE2E {
		t.Fatalf("left from Overview opened %+v", m.tab())
	}
	wantScreen(t, m, "No e2e run attached.")

	start := time.Now().Add(-time.Second)
	f.StartTest(start, e2etrack.Ref{Kind: e2etrack.KindNamespace, Name: monitortest.TestNamespace})
	f.Logs.Append(monitortest.Controller, start.Add(100*time.Millisecond).UTC().Format(time.RFC3339Nano)+` {"level":"INFO","msg":"Syncer: registering worker","pod":"abcd-123/counter-1"}`)
	f.Logs.Append(monitortest.Worker1, start.Add(200*time.Millisecond).UTC().Format(time.RFC3339Nano)+` {"level":"ERROR","msg":"restore failed"}`)
	m.Update(changeMsg(monitor.TopicE2E))

	wantScreen(t, m, "TestA", "namespace:abcd-123", "timeline", "lanes: test ate-controller workers/counter",
		"a_test.go:10: starting", "Syncer: registering worker", "restore failed")
	dontWantScreen(t, m, "E2E-MONITOR")
	if !strings.Contains(screen(m), "e2e ✓0 ✗0 ●1") {
		t.Errorf("header lacks the running count:\n%s", screen(m))
	}

	press(m, "t")
	wantScreen(t, m, "output", "a_test.go:10: starting")
	dontWantScreen(t, m, "restore failed")
	press(m, "t", "o")
	wantScreen(t, m, "timeline (own resources)")
}

func TestHelpAndTabs(t *testing.T) {
	m, _ := newModel(t)
	press(m, "?")
	wantScreen(t, m, "Keys", "cycle the minimum log level")
	press(m, "x") // Any key closes help.
	dontWantScreen(t, m, "Press any key to close.")
	press(m, "3")
	if m.tab().kind != tabWorkers {
		t.Errorf("3 opened %+v", m.tab())
	}
	if _, cmd := m.Update(key("q")); cmd == nil || cmd() != tea.Quit() {
		t.Errorf("q did not quit")
	}
}

func TestNarrowTerminal(t *testing.T) {
	m, _ := newModel(t)
	m.Update(tea.WindowSizeMsg{Width: 30, Height: 8})
	for _, line := range strings.Split(m.View(), "\n") {
		if w := ansi.StringWidth(line); w > 30 {
			t.Errorf("line wider than the terminal (%d): %q", w, ansi.Strip(line))
		}
	}
}
