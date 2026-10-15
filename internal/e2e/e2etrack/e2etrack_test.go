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

package e2etrack

import "testing"

func TestFormatParseRoundTrip(t *testing.T) {
	refs := []Ref{
		{Kind: KindNamespace, Name: "abcd-123"},
		{Kind: KindActor, Name: "counter", Atespace: "demo", UID: "u-1"},
		{Kind: KindWorkerPool, Name: "pool", Namespace: "ns"},
	}
	for _, ref := range refs {
		line := Format(ref)
		got, ok := Parse(line)
		if !ok || got != ref {
			t.Errorf("Parse(Format(%+v)) = %+v, %v", ref, got, ok)
		}
		// As written by t.Logf under go test -v.
		got, ok = Parse("    namespace.go:89: " + line + "\n")
		if !ok || got != ref {
			t.Errorf("Parse with Logf prefix = %+v, %v", got, ok)
		}
	}
}

func TestParseRejects(t *testing.T) {
	for _, line := range []string{
		"",
		"plain output",
		Marker + " not json",
		Marker + ` {"kind":"namespace"}`,
		Marker + ` {"name":"x"}`,
	} {
		if ref, ok := Parse(line); ok {
			t.Errorf("Parse(%q) = %+v, want no match", line, ref)
		}
	}
}
