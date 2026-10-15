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

// Package server serves the dashboard: a JSON API over discovery, logs, the
// e2e feed, and substrate state, server-sent events for live updates, and the
// embedded single-page UI.
package server

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/discovery"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/e2efeed"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/substrate"
)

//go:embed web
var webFS embed.FS

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

// Config wires the server to its sources.
type Config struct {
	Discovery *discovery.Discoverer
	Logs      *logs.Manager
	Feed      *e2efeed.Feed
	// Substrate may be nil when ateapi polling is disabled.
	Substrate     *substrate.Poller
	FollowWorkers FollowMode
	// Info is shown in the UI header.
	Info Info
}

// Info describes the monitored cluster.
type Info struct {
	Context         string     `json:"context"`
	SystemNamespace string     `json:"systemNamespace"`
	FollowWorkers   FollowMode `json:"followWorkers"`
}

// Server is the dashboard's HTTP server.
type Server struct {
	cfg Config
	mux *http.ServeMux
	// baseCtx scopes on-demand log streams, which outlive the request that
	// started them.
	baseCtx context.Context
}

// New builds a Server. ctx bounds the log streams it starts on demand.
func New(ctx context.Context, cfg Config) *Server {
	cfg.Info.FollowWorkers = cfg.FollowWorkers
	s := &Server{cfg: cfg, mux: http.NewServeMux(), baseCtx: ctx}
	s.routes()
	return s
}

// Handler returns the root handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	static, err := fs.Sub(webFS, "web")
	if err != nil {
		panic(err)
	}
	s.mux.Handle("GET /", http.FileServerFS(static))
	s.mux.HandleFunc("GET /api/info", s.handleInfo)
	s.mux.HandleFunc("GET /api/components", s.handleComponents)
	s.mux.HandleFunc("GET /api/components/{name}", s.handleComponent)
	s.mux.HandleFunc("GET /api/pools", s.handlePools)
	s.mux.HandleFunc("GET /api/namespaces", s.handleNamespaces)
	s.mux.HandleFunc("GET /api/streams", s.handleStreams)
	s.mux.HandleFunc("GET /api/logs", s.handleLogs)
	s.mux.HandleFunc("GET /api/logs/stream", s.handleLogStream)
	s.mux.HandleFunc("GET /api/logs/previous", s.handlePrevious)
	s.mux.HandleFunc("GET /api/e2e", s.handleE2E)
	s.mux.HandleFunc("GET /api/e2e/test", s.handleE2ETest)
	s.mux.HandleFunc("GET /api/e2e/package", s.handleE2EPackage)
	s.mux.HandleFunc("GET /api/e2e/timeline", s.handleTimeline)
	s.mux.HandleFunc("GET /api/substrate", s.handleSubstrate)
	s.mux.HandleFunc("GET /api/events", s.handleEvents)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Debug("writing response", "err", err)
	}
}

func (s *Server) handleInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.cfg.Info)
}

func (s *Server) handleComponents(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.cfg.Discovery.Components())
}

func (s *Server) handleComponent(w http.ResponseWriter, r *http.Request) {
	c, ok := s.cfg.Discovery.Component(r.PathValue("name"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, c)
}

func (s *Server) handlePools(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.cfg.Discovery.Pools())
}

func (s *Server) handleNamespaces(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.cfg.Discovery.Namespaces())
}

func (s *Server) handleStreams(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.cfg.Logs.Streams())
}

func (s *Server) handleSubstrate(w http.ResponseWriter, _ *http.Request) {
	if s.cfg.Substrate == nil {
		writeJSON(w, substrate.State{APIError: "ateapi polling is disabled (--substrate-api=false)"})
		return
	}
	writeJSON(w, s.cfg.Substrate.State())
}

// findPod looks a pod up among components and worker pools.
func (s *Server) findPod(namespace, name string) (discovery.Pod, bool) {
	for _, c := range s.cfg.Discovery.Components() {
		for _, p := range c.Pods {
			if p.Namespace == namespace && p.Name == name {
				return p, true
			}
		}
	}
	for _, pool := range s.cfg.Discovery.Pools() {
		for _, p := range pool.Pods {
			if p.Namespace == namespace && p.Name == name {
				return p, true
			}
		}
	}
	return discovery.Pod{}, false
}

// logFilter builds a filter from query parameters:
//
//	component=NAME       every pod of a component
//	pod=NS/NAME          one pod (followed on demand if it is not already)
//	container=NAME       with pod, one container
//	level=WARN           minimum level
//	q=TEXT               substring
//	since=RFC3339        lower time bound
func (s *Server) logFilter(r *http.Request) (logs.Filter, error) {
	q := r.URL.Query()
	f := logs.Filter{MinLevel: q.Get("level"), Contains: q.Get("q")}
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return f, fmt.Errorf("bad since: %w", err)
		}
		f.Since = t
	}
	if name := q.Get("component"); name != "" {
		c, ok := s.cfg.Discovery.Component(name)
		if !ok {
			return f, fmt.Errorf("unknown component %q", name)
		}
		f.Pods = map[string]bool{}
		for _, p := range c.Pods {
			f.Pods[logs.PodKey(p.Namespace, p.Name)] = true
		}
	}
	if key := q.Get("pod"); key != "" {
		ns, name, ok := strings.Cut(key, "/")
		if !ok {
			return f, fmt.Errorf("pod must be NAMESPACE/NAME")
		}
		var containers []string
		if c := q.Get("container"); c != "" {
			containers = []string{c}
		} else if p, ok := s.findPod(ns, name); ok {
			for _, c := range p.Containers {
				if c.LongRunning() {
					containers = append(containers, c.Name)
				}
			}
		}
		f.Pods = nil
		f.Sources = map[logs.Source]bool{}
		for _, c := range containers {
			src := logs.Source{Namespace: ns, Pod: name, Container: c}
			f.Sources[src] = true
			s.cfg.Logs.Touch(s.baseCtx, src)
		}
		if len(containers) == 0 {
			f.Pods = map[string]bool{key: true}
		}
	}
	return f, nil
}

func queryInt(r *http.Request, key string, def int) int {
	if v, err := strconv.Atoi(r.URL.Query().Get(key)); err == nil && v > 0 {
		return v
	}
	return def
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	f, err := s.logFilter(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, s.cfg.Logs.Query(f, queryInt(r, "limit", 1000)))
}

// handleLogStream sends the last `limit` matching lines, then every new one.
func (s *Server) handleLogStream(w http.ResponseWriter, r *http.Request) {
	f, err := s.logFilter(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ev, ok := newSSE(w)
	if !ok {
		return
	}
	// Subscribe before reading the backlog so no line falls between them.
	ch, cancel := s.cfg.Logs.Subscribe(f, 1024)
	defer cancel()
	var lastSeq uint64
	backlog := s.cfg.Logs.Query(f, queryInt(r, "limit", 1000))
	for _, l := range backlog {
		lastSeq = max(lastSeq, l.Seq)
	}
	if err := ev.send("backlog", backlog); err != nil {
		return
	}
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	batch := make([]logs.Line, 0, 64)
	flush := time.NewTicker(200 * time.Millisecond)
	defer flush.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case l := <-ch:
			if l.Seq > lastSeq {
				batch = append(batch, l)
			}
		case <-flush.C:
			if len(batch) > 0 {
				if err := ev.send("lines", batch); err != nil {
					return
				}
				batch = batch[:0]
			}
		case <-ping.C:
			if err := ev.comment("ping"); err != nil {
				return
			}
		}
	}
}

func (s *Server) handlePrevious(w http.ResponseWriter, r *http.Request) {
	ns, name, ok := strings.Cut(r.URL.Query().Get("pod"), "/")
	container := r.URL.Query().Get("container")
	if !ok || container == "" {
		http.Error(w, "pod=NAMESPACE/NAME and container are required", http.StatusBadRequest)
		return
	}
	text, err := s.cfg.Logs.Previous(r.Context(), logs.Source{Namespace: ns, Pod: name, Container: container}, int64(queryInt(r, "tail", 2000)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprint(w, text)
}

func (s *Server) handleE2E(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.cfg.Feed.Summary())
}

// TestDetail is a test with every resource it is tied to.
type TestDetail struct {
	e2efeed.Test
	AllRefs []refJSON `json:"allRefs"`
}

type refJSON struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
	Atespace  string `json:"atespace,omitempty"`
	UID       string `json:"uid,omitempty"`
}

func (s *Server) handleE2ETest(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	t, ok := s.cfg.Feed.Test(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	d := TestDetail{Test: t}
	for _, ref := range s.refsFor(t) {
		d.AllRefs = append(d.AllRefs, refJSON{string(ref.Kind), ref.Name, ref.Namespace, ref.Atespace, ref.UID})
	}
	writeJSON(w, d)
}

func (s *Server) handleE2EPackage(w http.ResponseWriter, r *http.Request) {
	p, ok := s.cfg.Feed.Package(r.URL.Query().Get("name"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, p)
}

// sseEvent topics the UI listens for.
const (
	topicComponents = "components"
	topicE2E        = "e2e"
	topicSubstrate  = "substrate"
)

// handleEvents tells the UI which data changed so it can refetch.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	ev, ok := newSSE(w)
	if !ok {
		return
	}
	disc, stopDisc := s.cfg.Discovery.Changes()
	defer stopDisc()
	feed, stopFeed := s.cfg.Feed.Changes()
	defer stopFeed()
	var sub <-chan struct{}
	if s.cfg.Substrate != nil {
		var stop func()
		sub, stop = s.cfg.Substrate.Changes()
		defer stop()
	}

	pending := map[string]bool{}
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-disc:
			pending[topicComponents] = true
		case <-feed:
			pending[topicE2E] = true
		case <-sub:
			pending[topicSubstrate] = true
		case <-tick.C:
			for topic := range pending {
				if err := ev.send(topic, struct{}{}); err != nil {
					return
				}
				delete(pending, topic)
			}
		case <-ping.C:
			if err := ev.comment("ping"); err != nil {
				return
			}
		}
	}
}

// RunFollower keeps the log manager following the right containers until ctx
// ends: every long-running container of every component, plus worker pods as
// FollowWorkers says.
func (s *Server) RunFollower(ctx context.Context) {
	disc, stopDisc := s.cfg.Discovery.Changes()
	defer stopDisc()
	feed, stopFeed := s.cfg.Feed.Changes()
	defer stopFeed()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	// Changes arrive in bursts; settle before resyncing.
	var settle <-chan time.Time
	s.cfg.Logs.Sync(ctx, s.desiredSources())
	for {
		select {
		case <-ctx.Done():
			s.cfg.Logs.Close()
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
			s.cfg.Logs.Sync(ctx, s.desiredSources())
		case <-tick.C:
			s.cfg.Logs.Sync(ctx, s.desiredSources())
			s.cfg.Logs.GC()
		}
	}
}

func (s *Server) desiredSources() map[logs.Source]bool {
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
	for _, c := range s.cfg.Discovery.Components() {
		for _, p := range c.Pods {
			addPod(p)
		}
	}
	if s.cfg.FollowWorkers == FollowNone {
		return want
	}
	active := s.activeTestNamespaces()
	for _, pool := range s.cfg.Discovery.Pools() {
		if s.cfg.FollowWorkers != FollowAll && !active[pool.Namespace] {
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
func (s *Server) activeTestNamespaces() map[string]bool {
	out := map[string]bool{}
	for _, ref := range s.cfg.Feed.ActiveRefs() {
		out[ref.Name] = true
		if ref.Namespace != "" {
			out[ref.Namespace] = true
		}
	}
	running := map[string]bool{}
	for _, t := range s.cfg.Feed.Summary().Tests {
		if t.State == e2efeed.StateRunning || t.State == e2efeed.StatePaused {
			running[t.Name] = true
		}
	}
	for _, ns := range s.cfg.Discovery.Namespaces() {
		if ns.Test != "" && running[ns.Test] {
			out[ns.Name] = true
		}
	}
	return out
}
