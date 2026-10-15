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

package e2efeed

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
)

const demo = "example.com/e2e/suites/demo"

func replay(t *testing.T) *Feed {
	t.Helper()
	f := New()
	if err := FollowFile(context.Background(), f, "testdata/run.json", false); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestReplay(t *testing.T) {
	f := replay(t)
	s := f.Summary()
	if s.Active {
		t.Errorf("replayed run still active")
	}
	want := Counts{Pass: 1, Fail: 3, Skip: 1, Total: 5}
	if s.Counts != want {
		t.Errorf("counts = %+v, want %+v", s.Counts, want)
	}
	states := map[string]string{}
	for _, tt := range s.Tests {
		states[tt.Name] = tt.State
		if tt.Output != nil {
			t.Errorf("summary kept output of %s", tt.Name)
		}
	}
	for name, state := range map[string]string{
		"TestDemo":         StateFail,
		"TestDemo/resume":  StateFail,
		"TestDemo/skipped": StateSkip,
		"TestHangs":        StateInterrupted,
		"TestOther":        StatePass,
	} {
		if states[name] != state {
			t.Errorf("%s state = %q, want %q", name, states[name], state)
		}
	}

	resume, ok := f.Test(TestID(demo, "TestDemo/resume"))
	if !ok {
		t.Fatal("TestDemo/resume missing")
	}
	if resume.Parent() != "TestDemo" || len(resume.Output) != 4 {
		t.Errorf("resume = %+v", resume)
	}
	if want := []e2etrack.Ref{{Kind: e2etrack.KindActor, Name: "counter-xyz", Atespace: "ate-e2e-demo"}}; !slices.Equal(resume.Refs, want) {
		t.Errorf("own refs = %+v", resume.Refs)
	}
	// Inherits the parent's atespace.
	if got := f.RefsFor(resume.ID()); len(got) != 2 {
		t.Errorf("RefsFor(resume) = %+v", got)
	}
	// The parent sees its subtest's actor.
	if got := f.RefsFor(TestID(demo, "TestDemo")); len(got) != 2 {
		t.Errorf("RefsFor(TestDemo) = %+v", got)
	}
	pkg, _ := f.Package(demo)
	if pkg.State != StateFail || len(pkg.Output) != 1 {
		t.Errorf("package = %+v", pkg)
	}
}

func TestIngestEchoAndNonJSON(t *testing.T) {
	f := New()
	var echo bytes.Buffer
	in := strings.Join([]string{
		"go: downloading something",
		`{"Action":"run","Package":"p","Test":"TestA"}`,
		`{"Action":"output","Package":"p","Test":"TestA","Output":"=== RUN   TestA\n"}`,
		`{"Action":"output","Package":"p","Test":"TestA","Output":"    x.go:1: E2E-MONITOR {\"kind\":\"namespace\",\"name\":\"abcd-123\"}\n"}`,
		`{"Action":"pass","Package":"p","Test":"TestA"}`,
	}, "\n")
	if err := f.Ingest(strings.NewReader(in), &echo); err != nil {
		t.Fatal(err)
	}
	if got, want := echo.String(), "go: downloading something\n=== RUN   TestA\n"; got != want {
		t.Errorf("echo = %q, want %q", got, want)
	}
	if p, ok := f.Package(""); !ok || len(p.Output) != 1 {
		t.Errorf("unattributed output = %+v", p)
	}
}

func TestActiveRefs(t *testing.T) {
	f := New()
	marker := e2etrack.Format(e2etrack.Ref{Kind: e2etrack.KindNamespace, Name: "abcd-123"})
	f.Apply(Event{Action: "run", Package: "p", Test: "TestA"})
	f.Apply(Event{Action: "output", Package: "p", Test: "TestA", Output: "    x.go:1: " + marker + "\n"})
	if got := f.ActiveRefs(); len(got) != 1 || got[0].Name != "abcd-123" {
		t.Errorf("ActiveRefs = %+v", got)
	}
	f.Apply(Event{Action: "pass", Package: "p", Test: "TestA"})
	if got := f.ActiveRefs(); len(got) != 0 {
		t.Errorf("ActiveRefs after pass = %+v", got)
	}
}

func TestInjectJSON(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"hack/run-e2e.sh", "hack/run-e2e.sh -json"},
		{"hack/run-e2e.sh ./internal/e2e/suites/demo -run TestDemo -args --preview=ALL", "hack/run-e2e.sh ./internal/e2e/suites/demo -run TestDemo -json -args --preview=ALL"},
		{"go test ./x -json -args --e2e", "go test ./x -json -args --e2e"},
		{"go test ./x -args -json", "go test ./x -json -args -json"},
	} {
		if got := strings.Join(InjectJSON(strings.Fields(tc.in)), " "); got != tc.want {
			t.Errorf("InjectJSON(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestRunCommand(t *testing.T) {
	// A shell stands in for go test: it ignores the injected -json.
	script := `echo '{"Action":"run","Package":"p","Test":"TestA"}'; echo '{"Action":"pass","Package":"p","Test":"TestA"}'`
	f := New()
	var echo, errOut bytes.Buffer
	if err := RunCommand(context.Background(), f, []string{"sh", "-c", script}, &echo, &errOut); err != nil {
		t.Fatalf("RunCommand: %v (stderr %q)", err, errOut.String())
	}
	s := f.Summary()
	if s.Active || s.Counts.Pass != 1 || !strings.Contains(s.Source, "-json") {
		t.Errorf("summary = %+v", s)
	}
}

func TestFollowFileTails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.json")
	f := New()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- FollowFile(ctx, f, path, true) }()

	// The file appears after the follower started, and grows.
	time.Sleep(100 * time.Millisecond)
	if err := os.WriteFile(path, []byte(`{"Action":"run","Package":"p","Test":"TestA"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return f.Summary().Counts.Running == 1 })
	fh, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	fh.WriteString(`{"Action":"pass","Package":"p","Test":"TestA"}` + "\n")
	fh.Close()
	waitFor(t, func() bool { return f.Summary().Counts.Pass == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Errorf("FollowFile: %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestMatcher(t *testing.T) {
	m := NewMatcher([]e2etrack.Ref{
		{Kind: e2etrack.KindNamespace, Name: "abcd-123"},
		{Kind: e2etrack.KindAtespace, Name: "demo"},
		{Kind: e2etrack.KindActor, Name: "counter-durable-1", Atespace: "demo", UID: "f07da53f-f96d"},
	})
	for _, tc := range []struct {
		raw   string
		attrs map[string]string
		want  bool
	}{
		{`{"msg":"x","pod":"abcd-123/counter-1"}`, nil, true},
		{`{"object":"bucket/atespaces/demo/actors/u/snap"}`, nil, true},
		{`{"req":{"atespace":"demo"}}`, nil, true},
		{`{"msg":"demo mode on"}`, nil, false},
		{`{"msg":"resumed counter-durable-1"}`, nil, true},
		{`{"actor":"f07da53f-f96d"}`, nil, true},
		{`{"msg":"x"}`, map[string]string{"ate.atespace": "demo"}, true},
		{`{"msg":"unrelated"}`, map[string]string{"ate.atespace": "other"}, false},
	} {
		if got := m.Match(logs.Line{Source: logs.Source{Namespace: "ate-system"}, Raw: tc.raw, Attrs: tc.attrs}); got != tc.want {
			t.Errorf("Match(%s, %v) = %v, want %v", tc.raw, tc.attrs, got, tc.want)
		}
	}
	// A worker pod in the test's namespace.
	if !m.Match(logs.Line{Source: logs.Source{Namespace: "abcd-123"}, Raw: "Count 3"}) {
		t.Errorf("line from a pod in a tracked namespace did not match")
	}
	if !NewMatcher(nil).Empty() {
		t.Errorf("empty matcher not Empty")
	}
}

func TestWriter(t *testing.T) {
	f := New()
	w := f.Writer()
	fmt.Fprint(w, "first line\nsecond ")
	fmt.Fprint(w, "line\npartial")
	p, ok := f.Package("")
	if !ok || len(p.Output) != 2 || p.Output[1].Text != "second line" {
		t.Errorf("unattributed output = %+v", p.Output)
	}
}
