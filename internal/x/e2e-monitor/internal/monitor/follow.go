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

package monitor

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/discovery"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/e2efeed"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
)

// RunFollower keeps the log manager following the right containers until ctx
// ends: every long-running container of every component, plus worker pods as
// FollowWorkers says.
func (m *Monitor) RunFollower(ctx context.Context) {
	disc, stopDisc := m.cfg.Discovery.Changes()
	defer stopDisc()
	feed, stopFeed := m.cfg.Feed.Changes()
	defer stopFeed()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	// Changes arrive in bursts; settle before resyncing.
	var settle <-chan time.Time
	m.cfg.Logs.Sync(ctx, m.desiredSources())
	for {
		select {
		case <-ctx.Done():
			m.cfg.Logs.Close()
			return
		case <-disc:
			if settle == nil {
				settle = time.After(time.Second)
			}
		case <-feed:
			if settle == nil {
				settle = time.After(time.Second)
			}
		case <-settle:
			settle = nil
			m.cfg.Logs.Sync(ctx, m.desiredSources())
		case <-tick.C:
			m.cfg.Logs.Sync(ctx, m.desiredSources())
			m.cfg.Logs.GC()
		}
	}
}

func (m *Monitor) desiredSources() map[logs.Source]bool {
	want := map[logs.Source]bool{}
	addPod := func(p discovery.Pod) {
		if p.Phase == string(corev1.PodSucceeded) || p.Phase == string(corev1.PodFailed) {
			return
		}
		for _, c := range p.Containers {
			if c.LongRunning() {
				want[logs.Source{Namespace: p.Namespace, Pod: p.Name, Container: c.Name}] = true
			}
		}
	}
	for _, c := range m.cfg.Discovery.Components() {
		for _, p := range c.Pods {
			addPod(p)
		}
	}
	if m.cfg.FollowWorkers == FollowNone {
		return want
	}
	active := m.activeTestNamespaces()
	for _, pool := range m.cfg.Discovery.Pools() {
		if m.cfg.FollowWorkers != FollowAll && !active[pool.Namespace] {
			continue
		}
		for _, p := range pool.Pods {
			addPod(p)
		}
	}
	return want
}

// activeTestNamespaces returns the namespaces of running tests: those they
// tracked, the atespaces they tracked (a fixture's atespace names its pool's
// namespace), and those annotated with a running test's name.
func (m *Monitor) activeTestNamespaces() map[string]bool {
	out := map[string]bool{}
	for _, ref := range m.cfg.Feed.ActiveRefs() {
		out[ref.Name] = true
		if ref.Namespace != "" {
			out[ref.Namespace] = true
		}
	}
	running := map[string]bool{}
	for _, t := range m.cfg.Feed.Summary().Tests {
		if t.State == e2efeed.StateRunning || t.State == e2efeed.StatePaused {
			running[t.Name] = true
		}
	}
	for _, ns := range m.cfg.Discovery.Namespaces() {
		if ns.Test != "" && running[ns.Test] {
			out[ns.Name] = true
		}
	}
	return out
}
