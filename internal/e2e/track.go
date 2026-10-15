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
	"testing"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
)

// TestAnnotation is set on namespaces CreateNamespace makes to the name of the
// test that made them.
const TestAnnotation = "ate.dev/e2e-test"

// Track records in the test log that t created or acted on ref, so that
// internal/x/e2e-monitor can correlate component logs with the test. The
// shared helpers call it; a test that creates resources directly can call it
// too.
func Track(t testing.TB, ref e2etrack.Ref) {
	t.Helper()
	t.Log(e2etrack.Format(ref))
}

// TrackActor is Track for an actor.
func TrackActor(t testing.TB, atespace, name string) {
	t.Helper()
	Track(t, e2etrack.Ref{Kind: e2etrack.KindActor, Atespace: atespace, Name: name})
}
