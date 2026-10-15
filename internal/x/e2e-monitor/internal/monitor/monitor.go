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

// Package monitor is the frontend-neutral core of e2e-monitor. It combines
// discovery, logs, the e2e feed, and substrate state into the read model every
// frontend (the web server, the TUI) presents, and owns the policy deciding
// which containers' logs are followed.
package monitor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/discovery"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/e2efeed"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/substrate"
)

// FollowMode selects which worker pods' logs are followed continuously.
type FollowMode string

const (
	// FollowE2E follows the worker pods of running e2e tests.
	FollowE2E FollowMode = "e2e"
	// FollowAll follows every worker pod.
	FollowAll FollowMode = "all"
	// FollowNone follows worker pods only while they are being viewed.
	FollowNone FollowMode = "none"
)

// ParseFollowMode validates a --follow-workers value.
func ParseFollowMode(s string) (FollowMode, error) {
	switch m := FollowMode(s); m {
	case FollowE2E, FollowAll, FollowNone:
		return m, nil
	}
	return "", fmt.Errorf("invalid follow mode %q: want e2e, all, or none", s)
}

// Info describes the monitored cluster.
type Info struct {
	Context         string     `json:"context"`
	SystemNamespace string     `json:"systemNamespace"`
	FollowWorkers   FollowMode `json:"followWorkers"`
}

// Config wires a Monitor to its sources.
type Config struct {
	Discovery *discovery.Discoverer
	Logs      *logs.Manager
	Feed      *e2efeed.Feed
	// Substrate may be nil when ateapi polling is disabled.
	Substrate     *substrate.Poller
	FollowWorkers FollowMode
	Info          Info
}

// Monitor is the read model shared by every frontend.
type Monitor struct {
	cfg Config
	// ctx scopes on-demand log streams, which outlive the view that asked
	// for them.
	ctx context.Context
}

// New builds a Monitor. ctx bounds the log streams it starts on demand.
func New(ctx context.Context, cfg Config) *Monitor {
	cfg.Info.FollowWorkers = cfg.FollowWorkers
	return &Monitor{cfg: cfg, ctx: ctx}
}

// Info describes the monitored cluster.
func (m *Monitor) Info() Info { return m.cfg.Info }

// Components returns every discovered component.
func (m *Monitor) Components() []discovery.Component { return m.cfg.Discovery.Components() }

// Component returns one component.
func (m *Monitor) Component(name string) (discovery.Component, bool) {
	return m.cfg.Discovery.Component(name)
}

// Pools returns every worker pool.
func (m *Monitor) Pools() []discovery.Pool { return m.cfg.Discovery.Pools() }

// Namespaces returns the e2e namespaces.
func (m *Monitor) Namespaces() []discovery.Namespace { return m.cfg.Discovery.Namespaces() }

// Streams lists the log streams.
func (m *Monitor) Streams() []logs.StreamInfo { return m.cfg.Logs.Streams() }

// Substrate returns the latest ateapi and router poll.
func (m *Monitor) Substrate() substrate.State {
	if m.cfg.Substrate == nil {
		return substrate.State{APIError: "ateapi polling is disabled (--substrate-api=false)"}
	}
	return m.cfg.Substrate.State()
}

// E2E returns the run summary.
func (m *Monitor) E2E() e2efeed.Summary { return m.cfg.Feed.Summary() }

// Package returns one test package with its output.
func (m *Monitor) Package(name string) (e2efeed.Package, bool) { return m.cfg.Feed.Package(name) }

// TestDetail is a test with every resource it is tied to.
type TestDetail struct {
	e2efeed.Test
	AllRefs []e2etrack.Ref `json:"allRefs"`
}

// Test returns one test with its output and resources.
func (m *Monitor) Test(id string) (TestDetail, bool) {
	t, ok := m.cfg.Feed.Test(id)
	if !ok {
		return TestDetail{}, false
	}
	return TestDetail{Test: t, AllRefs: m.refsFor(t)}, true
}

// FindPod looks a pod up among components and worker pools.
func (m *Monitor) FindPod(namespace, name string) (discovery.Pod, bool) {
	for _, c := range m.cfg.Discovery.Components() {
		for _, p := range c.Pods {
			if p.Namespace == namespace && p.Name == name {
				return p, true
			}
		}
	}
	for _, pool := range m.cfg.Discovery.Pools() {
		for _, p := range pool.Pods {
			if p.Namespace == namespace && p.Name == name {
				return p, true
			}
		}
	}
	return discovery.Pod{}, false
}

// LogQuery selects log lines.
type LogQuery struct {
	// Component selects every pod of a component.
	Component string
	// Pod ("namespace/name") selects one pod, followed on demand if it is
	// not already; it overrides Component.
	Pod string
	// Container narrows Pod to one container.
	Container string
	MinLevel  string
	Contains  string
	Since     time.Time
	// Limit caps the lines returned (the newest are kept); 0 means 1000.
	Limit int
}

func (q LogQuery) limit() int {
	if q.Limit <= 0 {
		return 1000
	}
	return q.Limit
}

// filter turns a query into a logs.Filter, following a named pod on demand.
func (m *Monitor) filter(q LogQuery) (logs.Filter, error) {
	f := logs.Filter{MinLevel: q.MinLevel, Contains: q.Contains, Since: q.Since}
	if q.Pod != "" {
		ns, name, ok := strings.Cut(q.Pod, "/")
		if !ok {
			return f, fmt.Errorf("pod must be NAMESPACE/NAME, got %q", q.Pod)
		}
		var containers []string
		if q.Container != "" {
			containers = []string{q.Container}
		} else if p, ok := m.FindPod(ns, name); ok {
			for _, c := range p.Containers {
				if c.LongRunning() {
					containers = append(containers, c.Name)
				}
			}
		}
		if len(containers) == 0 {
			// The pod is gone; its buffered lines are still worth showing.
			f.Pods = map[string]bool{q.Pod: true}
			return f, nil
		}
		f.Sources = map[logs.Source]bool{}
		for _, c := range containers {
			src := logs.Source{Namespace: ns, Pod: name, Container: c}
			f.Sources[src] = true
			m.cfg.Logs.Touch(m.ctx, src)
		}
		return f, nil
	}
	if q.Component != "" {
		c, ok := m.cfg.Discovery.Component(q.Component)
		if !ok {
			return f, fmt.Errorf("unknown component %q", q.Component)
		}
		f.Pods = map[string]bool{}
		for _, p := range c.Pods {
			f.Pods[logs.PodKey(p.Namespace, p.Name)] = true
		}
	}
	return f, nil
}

// Logs returns the newest lines matching q, oldest first.
func (m *Monitor) Logs(q LogQuery) ([]logs.Line, error) {
	f, err := m.filter(q)
	if err != nil {
		return nil, err
	}
	return m.cfg.Logs.Query(f, q.limit()), nil
}

// LogSubscription is a backlog of matching lines plus the live ones after it.
type LogSubscription struct {
	Backlog []logs.Line
	// C delivers new lines; lines with Seq <= LastSeq are already in Backlog
	// and must be skipped.
	C       <-chan logs.Line
	LastSeq uint64
	Cancel  func()
}

// Subscribe returns the newest lines matching q and then every new one until
// Cancel is called.
func (m *Monitor) Subscribe(q LogQuery, buffer int) (*LogSubscription, error) {
	f, err := m.filter(q)
	if err != nil {
		return nil, err
	}
	// Subscribe before reading the backlog so no line falls between them.
	ch, cancel := m.cfg.Logs.Subscribe(f, buffer)
	sub := &LogSubscription{C: ch, Cancel: cancel, Backlog: m.cfg.Logs.Query(f, q.limit())}
	for _, l := range sub.Backlog {
		sub.LastSeq = max(sub.LastSeq, l.Seq)
	}
	return sub, nil
}

// Previous returns the logs of the previous instance of a restarted
// container.
func (m *Monitor) Previous(ctx context.Context, src logs.Source, tail int64) (string, error) {
	return m.cfg.Logs.Previous(ctx, src, tail)
}

// Topic names a part of the read model that changed.
type Topic string

const (
	TopicComponents Topic = "components"
	TopicE2E        Topic = "e2e"
	TopicSubstrate  Topic = "substrate"
)

// changeInterval coalesces bursts of changes.
const changeInterval = 500 * time.Millisecond

// Changes delivers the topics that changed, at most once per topic every
// half second, until the returned func is called.
func (m *Monitor) Changes() (<-chan Topic, func()) {
	out := make(chan Topic, 3)
	disc, stopDisc := m.cfg.Discovery.Changes()
	feed, stopFeed := m.cfg.Feed.Changes()
	var sub <-chan struct{}
	stopSub := func() {}
	if m.cfg.Substrate != nil {
		sub, stopSub = m.cfg.Substrate.Changes()
	}
	done := make(chan struct{})
	go func() {
		defer stopDisc()
		defer stopFeed()
		defer stopSub()
		pending := map[Topic]bool{}
		tick := time.NewTicker(changeInterval)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-m.ctx.Done():
				return
			case <-disc:
				pending[TopicComponents] = true
			case <-feed:
				pending[TopicE2E] = true
			case <-sub:
				pending[TopicSubstrate] = true
			case <-tick.C:
				for topic := range pending {
					select {
					case out <- topic:
						delete(pending, topic)
					default:
						// The consumer is behind; retry next tick.
					}
				}
			}
		}
	}()
	var once sync.Once
	return out, func() { once.Do(func() { close(done) }) }
}
