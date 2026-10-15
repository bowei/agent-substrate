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

// Package e2etrack defines the marker an e2e test writes to its log when it
// creates a cluster or substrate resource, so that tools reading the test's
// `go test -json` stream (internal/x/e2e-monitor) can tie that resource, and
// every component log line that mentions it, back to the test.
//
// The marker carries no test name: test2json already attributes each t.Log
// line to the test that wrote it.
package e2etrack

import (
	"encoding/json"
	"strings"
)

// Marker starts every tracking line.
const Marker = "E2E-MONITOR"

// Kind is the kind of resource a Ref names.
type Kind string

const (
	// KindNamespace is a Kubernetes namespace.
	KindNamespace Kind = "namespace"
	// KindAtespace is a substrate atespace.
	KindAtespace Kind = "atespace"
	// KindActor is a substrate actor; Atespace is set.
	KindActor Kind = "actor"
	// KindWorkerPool is a WorkerPool; Namespace is set.
	KindWorkerPool Kind = "workerpool"
)

// Ref names one resource a test created or acted on.
type Ref struct {
	Kind      Kind   `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
	Atespace  string `json:"atespace,omitempty"`
	UID       string `json:"uid,omitempty"`
}

// Format returns the tracking line for ref, without a trailing newline.
func Format(ref Ref) string {
	b, err := json.Marshal(ref)
	if err != nil {
		// Ref holds only strings; Marshal cannot fail.
		panic(err)
	}
	return Marker + " " + string(b)
}

// Parse finds a tracking marker in line, which may carry the file:line prefix
// that testing.T.Logf adds, and returns the Ref it carries.
func Parse(line string) (Ref, bool) {
	i := strings.Index(line, Marker+" {")
	if i < 0 {
		return Ref{}, false
	}
	payload := strings.TrimSpace(line[i+len(Marker)+1:])
	var ref Ref
	if err := json.Unmarshal([]byte(payload), &ref); err != nil {
		return Ref{}, false
	}
	if ref.Kind == "" || ref.Name == "" {
		return Ref{}, false
	}
	return ref, true
}
