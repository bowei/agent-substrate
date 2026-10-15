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
	"strings"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
)

// minNeedle is the shortest name matched as a bare substring; shorter names
// ("demo") would match unrelated lines, so they are only matched in
// key/value or path context.
const minNeedle = 8

// Matcher decides whether a component log line is about any of a set of
// tracked resources. Substrate's components do not tag every line with
// structured attributes, so it also looks for the names in the raw line, as
// they appear in object paths ("ns/atespaces/a/actors/..."), pod references
// ("ns/pod"), and request dumps (`"atespace":"a"`). Every line from a pod in a
// tracked namespace (the test's worker pods) matches too.
type Matcher struct {
	needles    []string
	atespaces  map[string]bool
	actors     map[string]bool
	namespaces map[string]bool
}

// NewMatcher builds a Matcher for refs; it matches nothing when refs is empty.
func NewMatcher(refs []e2etrack.Ref) *Matcher {
	m := &Matcher{atespaces: map[string]bool{}, actors: map[string]bool{}, namespaces: map[string]bool{}}
	seen := map[string]bool{}
	add := func(s string) {
		if s != "" && !seen[s] {
			seen[s] = true
			m.needles = append(m.needles, s)
		}
	}
	addName := func(name string, contexts ...string) {
		if name == "" {
			return
		}
		if len(name) >= minNeedle {
			add(name)
			return
		}
		for _, c := range contexts {
			add(strings.ReplaceAll(c, "%s", name))
		}
	}
	for _, r := range refs {
		switch r.Kind {
		case e2etrack.KindNamespace:
			m.namespaces[r.Name] = true
			addName(r.Name, `"namespace":"%s"`, `"%s/`, ` %s/`, `namespace=%s`)
		case e2etrack.KindAtespace:
			m.atespaces[r.Name] = true
			addName(r.Name, `"atespace":"%s"`, `atespaces/%s/`, `atespace=%s`)
		case e2etrack.KindActor:
			m.actors[r.Name] = true
			addName(r.Name, `actors/%s`, `"name":"%s"`, `actor=%s`)
			add(r.UID)
		case e2etrack.KindWorkerPool:
			m.namespaces[r.Namespace] = true
			addName(r.Namespace, `"%s/`, ` %s/`)
		}
	}
	return m
}

// Empty reports whether the matcher can match anything.
func (m *Matcher) Empty() bool {
	return len(m.needles) == 0 && len(m.atespaces) == 0 && len(m.actors) == 0 && len(m.namespaces) == 0
}

// Match reports whether l mentions a tracked resource.
func (m *Matcher) Match(l logs.Line) bool {
	if m.namespaces[l.Namespace] {
		return true
	}
	if a, ok := l.Attrs["ate.atespace"]; ok && m.atespaces[a] {
		return true
	}
	if a, ok := l.Attrs["ate.actor.name"]; ok && m.actors[a] {
		return true
	}
	for _, n := range m.needles {
		if strings.Contains(l.Raw, n) {
			return true
		}
	}
	return false
}
