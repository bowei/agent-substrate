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

package e2e

import (
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
)

// logRecorder captures what Track logs.
type logRecorder struct {
	testing.TB
	lines []string
}

func (r *logRecorder) Helper() {}

func (r *logRecorder) Log(args ...any) { r.lines = append(r.lines, fmt.Sprint(args...)) }

func TestTrackActor(t *testing.T) {
	r := &logRecorder{TB: t}
	TrackActor(r, "demo", "counter-1")
	if len(r.lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(r.lines))
	}
	ref, ok := e2etrack.Parse(r.lines[0])
	if !ok {
		t.Fatalf("logged line %q is not a tracking marker", r.lines[0])
	}
	want := e2etrack.Ref{Kind: e2etrack.KindActor, Atespace: "demo", Name: "counter-1"}
	if ref != want {
		t.Errorf("got %+v, want %+v", ref, want)
	}
}
