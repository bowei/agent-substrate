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

// Package server is the web frontend: a JSON API and server-sent events over
// the monitor's read model, and the embedded single-page UI.
package server

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/monitor"
)

//go:embed web
var webFS embed.FS

// Server is the dashboard's HTTP server.
type Server struct {
	m   *monitor.Monitor
	mux *http.ServeMux
}

// New builds a Server over m.
func New(m *monitor.Monitor) *Server {
	s := &Server{m: m, mux: http.NewServeMux()}
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
	s.mux.HandleFunc("GET /api/info", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.m.Info()) })
	s.mux.HandleFunc("GET /api/components", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.m.Components()) })
	s.mux.HandleFunc("GET /api/components/{name}", s.handleComponent)
	s.mux.HandleFunc("GET /api/pools", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.m.Pools()) })
	s.mux.HandleFunc("GET /api/namespaces", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.m.Namespaces()) })
	s.mux.HandleFunc("GET /api/streams", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.m.Streams()) })
	s.mux.HandleFunc("GET /api/substrate", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.m.Substrate()) })
	s.mux.HandleFunc("GET /api/logs", s.handleLogs)
	s.mux.HandleFunc("GET /api/logs/stream", s.handleLogStream)
	s.mux.HandleFunc("GET /api/logs/previous", s.handlePrevious)
	s.mux.HandleFunc("GET /api/e2e", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.m.E2E()) })
	s.mux.HandleFunc("GET /api/e2e/test", s.handleE2ETest)
	s.mux.HandleFunc("GET /api/e2e/package", s.handleE2EPackage)
	s.mux.HandleFunc("GET /api/e2e/timeline", s.handleTimeline)
	s.mux.HandleFunc("GET /api/events", s.handleEvents)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Debug("writing response", "err", err)
	}
}

func queryInt(r *http.Request, key string, def int) int {
	if v, err := strconv.Atoi(r.URL.Query().Get(key)); err == nil && v > 0 {
		return v
	}
	return def
}

func (s *Server) handleComponent(w http.ResponseWriter, r *http.Request) {
	c, ok := s.m.Component(r.PathValue("name"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, c)
}

// logQuery reads a monitor.LogQuery from query parameters: component, pod
// (NAMESPACE/NAME), container, level, q (substring), since (RFC 3339), limit.
func logQuery(r *http.Request) (monitor.LogQuery, error) {
	v := r.URL.Query()
	q := monitor.LogQuery{
		Component: v.Get("component"),
		Pod:       v.Get("pod"),
		Container: v.Get("container"),
		MinLevel:  v.Get("level"),
		Contains:  v.Get("q"),
		Limit:     queryInt(r, "limit", 1000),
	}
	if since := v.Get("since"); since != "" {
		t, err := time.Parse(time.RFC3339Nano, since)
		if err != nil {
			return q, fmt.Errorf("bad since: %w", err)
		}
		q.Since = t
	}
	return q, nil
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	q, err := logQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	lines, err := s.m.Logs(q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, lines)
}

// handleLogStream sends the newest matching lines as a "backlog" event, then
// new ones in "lines" batches.
func (s *Server) handleLogStream(w http.ResponseWriter, r *http.Request) {
	q, err := logQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sub, err := s.m.Subscribe(q, 1024)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer sub.Cancel()
	ev, ok := newSSE(w)
	if !ok {
		return
	}
	if err := ev.send("backlog", sub.Backlog); err != nil {
		return
	}
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	flush := time.NewTicker(200 * time.Millisecond)
	defer flush.Stop()
	batch := make([]logs.Line, 0, 64)
	for {
		select {
		case <-r.Context().Done():
			return
		case l := <-sub.C:
			if l.Seq > sub.LastSeq {
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
	text, err := s.m.Previous(r.Context(), logs.Source{Namespace: ns, Pod: name, Container: container}, int64(queryInt(r, "tail", 2000)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprint(w, text)
}

func (s *Server) handleE2ETest(w http.ResponseWriter, r *http.Request) {
	t, ok := s.m.Test(r.URL.Query().Get("id"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, t)
}

func (s *Server) handleE2EPackage(w http.ResponseWriter, r *http.Request) {
	p, ok := s.m.Package(r.URL.Query().Get("name"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, p)
}

// handleTimeline serves a test's timeline; scope=own correlates only the
// resources the test tracked itself.
func (s *Server) handleTimeline(w http.ResponseWriter, r *http.Request) {
	tl, ok := s.m.Timeline(r.URL.Query().Get("id"), monitor.TimelineOptions{
		Own:   r.URL.Query().Get("scope") == "own",
		Limit: queryInt(r, "limit", 3000),
	})
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, tl)
}

// handleEvents tells the UI which data changed so it can refetch.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	ev, ok := newSSE(w)
	if !ok {
		return
	}
	changes, stop := s.m.Changes()
	defer stop()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case topic := <-changes:
			if err := ev.send(string(topic), struct{}{}); err != nil {
				return
			}
		case <-ping.C:
			if err := ev.comment("ping"); err != nil {
				return
			}
		}
	}
}
