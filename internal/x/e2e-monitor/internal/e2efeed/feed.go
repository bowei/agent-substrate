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

// Package e2efeed follows an e2e run through its `go test -json` (test2json)
// event stream: it keeps the state and output of every test and collects the
// resources each test tracked with internal/e2e/e2etrack.
package e2efeed

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
)

// Event is one test2json event.
type Event struct {
	Time       time.Time `json:"Time"`
	Action     string    `json:"Action"`
	Package    string    `json:"Package"`
	ImportPath string    `json:"ImportPath"`
	Test       string    `json:"Test"`
	Elapsed    float64   `json:"Elapsed"`
	Output     string    `json:"Output"`
}

// Test states.
const (
	StateRunning     = "running"
	StatePaused      = "paused"
	StatePass        = "pass"
	StateFail        = "fail"
	StateSkip        = "skip"
	StateInterrupted = "interrupted"
)

// maxOutput bounds the output kept per test or package.
const maxOutput = 5000

// OutputLine is one line of test output.
type OutputLine struct {
	Time time.Time `json:"time"`
	Text string    `json:"text"`
}

// Test is one test or subtest.
type Test struct {
	Package string    `json:"package"`
	Name    string    `json:"name"`
	State   string    `json:"state"`
	Start   time.Time `json:"start"`
	End     time.Time `json:"end,omitzero"`
	Elapsed float64   `json:"elapsed"`
	// Refs are the resources the test (not its subtests) tracked.
	Refs    []e2etrack.Ref `json:"refs,omitempty"`
	Output  []OutputLine   `json:"output,omitempty"`
	Dropped int            `json:"dropped,omitempty"`
	// Current is the most recent output line, a hint of what the test is
	// doing now.
	Current string `json:"current,omitempty"`
}

// ID is the test's key: "package.TestName/sub".
func (t *Test) ID() string { return TestID(t.Package, t.Name) }

// TestID builds a test key.
func TestID(pkg, name string) string { return pkg + "." + name }

// Parent returns the name of the enclosing test, or "" for a top-level test.
func (t *Test) Parent() string {
	if i := strings.LastIndex(t.Name, "/"); i >= 0 {
		return t.Name[:i]
	}
	return ""
}

// Package is one test binary's run.
type Package struct {
	Name    string       `json:"name"`
	State   string       `json:"state"`
	Start   time.Time    `json:"start"`
	End     time.Time    `json:"end,omitzero"`
	Elapsed float64      `json:"elapsed"`
	Output  []OutputLine `json:"output,omitempty"`
	Dropped int          `json:"dropped,omitempty"`
}

// Counts tallies test states.
type Counts struct {
	Running int `json:"running"`
	Pass    int `json:"pass"`
	Fail    int `json:"fail"`
	Skip    int `json:"skip"`
	Total   int `json:"total"`
}

// Summary is the run without per-test output.
type Summary struct {
	Source   string    `json:"source"`
	Active   bool      `json:"active"`
	Start    time.Time `json:"start,omitzero"`
	End      time.Time `json:"end,omitzero"`
	ExitErr  string    `json:"exitErr,omitempty"`
	Counts   Counts    `json:"counts"`
	Packages []Package `json:"packages"`
	Tests    []Test    `json:"tests"`
}

// Feed holds one run's state.
type Feed struct {
	mu       sync.Mutex
	source   string
	active   bool
	start    time.Time
	end      time.Time
	exitErr  string
	packages map[string]*Package
	tests    map[string]*Test
	order    []string
	subs     map[chan struct{}]struct{}
	now      func() time.Time
}

// New returns an empty Feed.
func New() *Feed {
	return &Feed{
		packages: map[string]*Package{},
		tests:    map[string]*Test{},
		subs:     map[chan struct{}]struct{}{},
		now:      time.Now,
	}
}

// Begin resets the feed for a new run read from source.
func (f *Feed) Begin(source string) {
	f.mu.Lock()
	f.source = source
	f.active = true
	f.start = f.now()
	f.end = time.Time{}
	f.exitErr = ""
	f.packages = map[string]*Package{}
	f.tests = map[string]*Test{}
	f.order = nil
	f.mu.Unlock()
	f.signal()
}

// Finish marks the run over; tests still running are marked interrupted.
func (f *Feed) Finish(err error) {
	f.mu.Lock()
	f.active = false
	f.end = f.now()
	if err != nil {
		f.exitErr = err.Error()
	}
	for _, t := range f.tests {
		if t.State == StateRunning || t.State == StatePaused {
			t.State = StateInterrupted
			t.End = f.end
		}
	}
	for _, p := range f.packages {
		if p.State == StateRunning {
			p.State = StateInterrupted
			p.End = f.end
		}
	}
	f.mu.Unlock()
	f.signal()
}

// Changes returns a channel that receives coalesced change notifications.
func (f *Feed) Changes() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	f.mu.Lock()
	f.subs[ch] = struct{}{}
	f.mu.Unlock()
	return ch, func() {
		f.mu.Lock()
		delete(f.subs, ch)
		f.mu.Unlock()
	}
}

func (f *Feed) signal() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for ch := range f.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Ingest reads test2json lines from r until it ends. Lines that are not JSON
// events (build errors, a wrapper script's own output) become package-less
// output. Every line's human-readable text is copied to echo when non-nil.
func (f *Feed) Ingest(r io.Reader, echo io.Writer) error {
	br := bufio.NewReaderSize(r, 64*1024)
	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			f.ingestLine(strings.TrimRight(line, "\r\n"), echo)
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (f *Feed) ingestLine(line string, echo io.Writer) {
	var ev Event
	if strings.HasPrefix(line, "{") && json.Unmarshal([]byte(line), &ev) == nil && ev.Action != "" {
		f.Apply(ev)
		// Tracking markers are for the dashboard, not the terminal.
		if _, marker := e2etrack.Parse(ev.Output); echo != nil && ev.Output != "" && !marker {
			fmt.Fprint(echo, ev.Output)
		}
		return
	}
	f.Apply(Event{Time: f.now(), Action: "output", Output: line + "\n"})
	if echo != nil {
		fmt.Fprintln(echo, line)
	}
}

// Apply applies one event.
func (f *Feed) Apply(ev Event) {
	if ev.Time.IsZero() {
		ev.Time = f.now()
	}
	pkgName := ev.Package
	if pkgName == "" {
		pkgName = ev.ImportPath
	}
	f.mu.Lock()
	if !f.active && f.start.IsZero() {
		f.active = true
		f.start = ev.Time
	}
	pkg := f.packageLocked(pkgName, ev.Time)
	if ev.Test == "" {
		switch ev.Action {
		case "output", "build-output":
			appendOutput(&pkg.Output, &pkg.Dropped, ev.Time, ev.Output)
		case "pass", "fail", "skip":
			pkg.State, pkg.End, pkg.Elapsed = ev.Action, ev.Time, ev.Elapsed
			f.interruptLocked(pkgName, ev.Time)
		case "build-fail":
			pkg.State, pkg.End = StateFail, ev.Time
		}
		f.mu.Unlock()
		f.signal()
		return
	}

	t := f.testLocked(pkgName, ev.Test, ev.Time)
	switch ev.Action {
	case "run", "cont":
		t.State = StateRunning
	case "pause":
		t.State = StatePaused
	case "pass", "fail", "skip":
		t.State, t.End, t.Elapsed = ev.Action, ev.Time, ev.Elapsed
		t.Current = ""
	case "output":
		appendOutput(&t.Output, &t.Dropped, ev.Time, ev.Output)
		text := strings.TrimSpace(ev.Output)
		if ref, ok := e2etrack.Parse(text); ok {
			if !slices.Contains(t.Refs, ref) {
				t.Refs = append(t.Refs, ref)
			}
		} else if text != "" && !strings.HasPrefix(text, "===") && !strings.HasPrefix(text, "---") {
			t.Current = text
		}
	}
	f.mu.Unlock()
	f.signal()
}

// interruptLocked marks tests of a finished package that never reported a
// result, as when the binary panics or times out.
func (f *Feed) interruptLocked(pkg string, at time.Time) {
	for _, t := range f.tests {
		if t.Package == pkg && (t.State == StateRunning || t.State == StatePaused) {
			t.State, t.End = StateInterrupted, at
		}
	}
}

func (f *Feed) packageLocked(name string, at time.Time) *Package {
	p, ok := f.packages[name]
	if !ok {
		p = &Package{Name: name, State: StateRunning, Start: at}
		f.packages[name] = p
	}
	return p
}

func (f *Feed) testLocked(pkg, name string, at time.Time) *Test {
	id := TestID(pkg, name)
	t, ok := f.tests[id]
	if !ok {
		t = &Test{Package: pkg, Name: name, State: StateRunning, Start: at}
		f.tests[id] = t
		f.order = append(f.order, id)
	}
	return t
}

func appendOutput(out *[]OutputLine, dropped *int, at time.Time, text string) {
	for _, line := range strings.SplitAfter(text, "\n") {
		line = strings.TrimRight(line, "\n")
		if line == "" {
			continue
		}
		*out = append(*out, OutputLine{Time: at, Text: line})
	}
	if over := len(*out) - maxOutput; over > 0 {
		*out = slices.Delete(*out, 0, over)
		*dropped += over
	}
}

// Summary returns the run's state with output stripped.
func (f *Feed) Summary() Summary {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := Summary{Source: f.source, Active: f.active, Start: f.start, End: f.end, ExitErr: f.exitErr}
	for _, p := range f.packages {
		if p.Name == "" && len(p.Output) == 0 {
			continue
		}
		cp := *p
		cp.Output = nil
		s.Packages = append(s.Packages, cp)
	}
	sort.Slice(s.Packages, func(i, j int) bool { return s.Packages[i].Name < s.Packages[j].Name })
	for _, id := range f.order {
		t := *f.tests[id]
		t.Output = nil
		s.Tests = append(s.Tests, t)
		switch t.State {
		case StateRunning, StatePaused:
			s.Counts.Running++
		case StatePass:
			s.Counts.Pass++
		case StateFail, StateInterrupted:
			s.Counts.Fail++
		case StateSkip:
			s.Counts.Skip++
		}
		s.Counts.Total++
	}
	return s
}

// Test returns a copy of one test, with output.
func (f *Feed) Test(id string) (Test, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tests[id]
	if !ok {
		return Test{}, false
	}
	cp := *t
	cp.Output = slices.Clone(t.Output)
	cp.Refs = slices.Clone(t.Refs)
	return cp, true
}

// Package returns a copy of one package, with output.
func (f *Feed) Package(name string) (Package, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.packages[name]
	if !ok {
		return Package{}, false
	}
	cp := *p
	cp.Output = slices.Clone(p.Output)
	return cp, true
}

// RefsFor returns the resources relevant to a test: its own, its ancestors'
// (a parent usually deploys the fixture its subtests use), and its
// descendants'.
func (f *Feed) RefsFor(id string) []e2etrack.Ref {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tests[id]
	if !ok {
		return nil
	}
	var out []e2etrack.Ref
	add := func(refs []e2etrack.Ref) {
		for _, r := range refs {
			if !slices.Contains(out, r) {
				out = append(out, r)
			}
		}
	}
	for _, o := range f.tests {
		if o.Package != t.Package {
			continue
		}
		if o.Name == t.Name || strings.HasPrefix(t.Name, o.Name+"/") || strings.HasPrefix(o.Name, t.Name+"/") {
			add(o.Refs)
		}
	}
	return out
}

// ActiveRefs returns the resources of every test still running.
func (f *Feed) ActiveRefs() []e2etrack.Ref {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []e2etrack.Ref
	for _, t := range f.tests {
		if t.State == StateRunning || t.State == StatePaused {
			out = append(out, t.Refs...)
		}
	}
	return out
}

// AllRefs returns the resources every test tracked, keyed by test ID.
func (f *Feed) AllRefs() map[string][]e2etrack.Ref {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string][]e2etrack.Ref{}
	for id, t := range f.tests {
		if len(t.Refs) > 0 {
			out[id] = slices.Clone(t.Refs)
		}
	}
	return out
}

// Writer returns an io.Writer whose lines become package-less output, for a
// command's stderr when nothing else displays it.
func (f *Feed) Writer() io.Writer { return &lineWriter{f: f} }

type lineWriter struct {
	mu  sync.Mutex
	f   *Feed
	buf []byte
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.f.ingestLine(strings.TrimRight(string(w.buf[:i]), "\r"), nil)
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}
