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

// Package substrate polls substrate's own view of the system: workers and
// actors from ateapi, and the router's /statusz. Everything here is
// best-effort; a failure is reported in the state, never fatal.
package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Worker is ateapi's record of a worker.
type Worker struct {
	Name         string   `json:"name"`
	Namespace    string   `json:"namespace"`
	Pool         string   `json:"pool"`
	Pod          string   `json:"pod"`
	Node         string   `json:"node,omitempty"`
	SandboxClass string   `json:"sandboxClass,omitempty"`
	State        string   `json:"state"`
	Actors       []string `json:"actors,omitempty"`
}

// Actor is ateapi's record of an actor.
type Actor struct {
	Atespace  string    `json:"atespace"`
	Name      string    `json:"name"`
	UID       string    `json:"uid"`
	Template  string    `json:"template,omitempty"`
	State     string    `json:"state"`
	WorkerPod string    `json:"workerPod,omitempty"`
	Node      string    `json:"node,omitempty"`
	Crash     string    `json:"crash,omitempty"`
	Updated   time.Time `json:"updated,omitzero"`
}

// State is the latest poll.
type State struct {
	Updated     time.Time       `json:"updated,omitzero"`
	APIError    string          `json:"apiError,omitempty"`
	Workers     []Worker        `json:"workers"`
	Actors      []Actor         `json:"actors"`
	RouterError string          `json:"routerError,omitempty"`
	Router      json.RawMessage `json:"router,omitempty"`
}

// Lister is the part of ateapipb.ControlClient the poller uses.
type Lister interface {
	ListWorkers(ctx context.Context, in *ateapipb.ListWorkersRequest, opts ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error)
	ListActors(ctx context.Context, in *ateapipb.ListActorsRequest, opts ...grpc.CallOption) (*ateapipb.ListActorsResponse, error)
}

// Dialers connect lazily so the dashboard starts even when ateapi or the
// router are unreachable, and reconnect after failures.
type (
	// APIDialer returns a client for ateapi and a func to close it.
	APIDialer func(ctx context.Context) (Lister, func(), error)
	// RouterDialer returns the base URL of the router's status server and a
	// func to close the connection to it.
	RouterDialer func(ctx context.Context) (string, func(), error)
)

// Poller polls ateapi and the router.
type Poller struct {
	dialAPI    APIDialer
	dialRouter RouterDialer
	interval   time.Duration
	http       *http.Client

	mu    sync.Mutex
	state State
	subs  map[chan struct{}]struct{}
}

// NewPoller returns a Poller; either dialer may be nil to skip that source.
func NewPoller(dialAPI APIDialer, dialRouter RouterDialer, interval time.Duration) *Poller {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Poller{
		dialAPI:    dialAPI,
		dialRouter: dialRouter,
		interval:   interval,
		http:       &http.Client{Timeout: 10 * time.Second},
		subs:       map[chan struct{}]struct{}{},
	}
}

// State returns the latest poll.
func (p *Poller) State() State {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state
}

// Changes returns a channel notified after every poll.
func (p *Poller) Changes() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	p.mu.Lock()
	p.subs[ch] = struct{}{}
	p.mu.Unlock()
	return ch, func() {
		p.mu.Lock()
		delete(p.subs, ch)
		p.mu.Unlock()
	}
}

// Run polls until ctx ends.
func (p *Poller) Run(ctx context.Context) {
	var api Lister
	var closeAPI, closeRouter func()
	var routerURL string
	defer func() {
		if closeAPI != nil {
			closeAPI()
		}
		if closeRouter != nil {
			closeRouter()
		}
	}()

	for {
		next := State{Updated: time.Now()}

		if p.dialAPI != nil {
			if api == nil {
				var err error
				api, closeAPI, err = p.dialAPI(ctx)
				if err != nil {
					api, closeAPI = nil, nil
					next.APIError = "connecting to ateapi: " + err.Error()
				}
			}
			if api != nil {
				workers, actors, err := poll(ctx, api)
				if err != nil {
					next.APIError = err.Error()
					// Reconnect next time; the port-forward may have died.
					closeAPI()
					api, closeAPI = nil, nil
				}
				next.Workers, next.Actors = workers, actors
			}
		}

		if p.dialRouter != nil {
			if routerURL == "" {
				var err error
				routerURL, closeRouter, err = p.dialRouter(ctx)
				if err != nil {
					routerURL, closeRouter = "", nil
					next.RouterError = "connecting to router: " + err.Error()
				}
			}
			if routerURL != "" {
				raw, err := p.statusz(ctx, routerURL)
				if err != nil {
					next.RouterError = err.Error()
					closeRouter()
					routerURL, closeRouter = "", nil
				}
				next.Router = raw
			}
		}

		p.mu.Lock()
		p.state = next
		for ch := range p.subs {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		p.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-time.After(p.interval):
		}
	}
}

func (p *Poller) statusz(ctx context.Context, base string) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/statusz?format=json", nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("statusz returned %d", resp.StatusCode)
	}
	if !json.Valid(body) {
		return nil, fmt.Errorf("statusz returned invalid JSON")
	}
	return body, nil
}

const maxPages = 50

func poll(ctx context.Context, api Lister) ([]Worker, []Actor, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	var workers []Worker
	token := ""
	for range maxPages {
		resp, err := api.ListWorkers(ctx, &ateapipb.ListWorkersRequest{Options: &ateapipb.ListOptions{PageSize: 500, PageToken: token}})
		if err != nil {
			return nil, nil, fmt.Errorf("ListWorkers: %w", err)
		}
		for _, w := range resp.GetWorkers() {
			workers = append(workers, Worker{
				Name:         w.GetMetadata().GetName(),
				Namespace:    w.GetWorkerNamespace(),
				Pool:         w.GetWorkerPool(),
				Pod:          w.GetWorkerPod(),
				Node:         w.GetNodeName(),
				SandboxClass: w.GetSandboxClass(),
				State:        trimEnum(w.GetStatus().GetState().String(), "WORKER_STATE_"),
			})
		}
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}

	var actors []Actor
	token = ""
	for range maxPages {
		resp, err := api.ListActors(ctx, &ateapipb.ListActorsRequest{Options: &ateapipb.ListOptions{PageSize: 500, PageToken: token}})
		if err != nil {
			return workers, nil, fmt.Errorf("ListActors: %w", err)
		}
		for _, a := range resp.GetActors() {
			md := a.GetMetadata()
			st := a.GetStatus()
			actor := Actor{
				Atespace:  md.GetAtespace(),
				Name:      md.GetName(),
				UID:       md.GetUid(),
				Template:  a.GetActorTemplate().GetName(),
				State:     trimEnum(st.GetState().String(), "ACTOR_STATE_"),
				WorkerPod: joinNonEmpty(st.GetWorkerAssignment().GetWorkerNamespace(), st.GetWorkerAssignment().GetWorkerPod()),
				Node:      st.GetAssignedNode(),
				Crash:     st.GetCrash().GetMessage(),
			}
			if ts := md.GetUpdateTime(); ts != nil {
				actor.Updated = ts.AsTime()
			}
			actors = append(actors, actor)
		}
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}

	byPod := map[string]*Worker{}
	for i := range workers {
		byPod[workers[i].Namespace+"/"+workers[i].Pod] = &workers[i]
	}
	for _, a := range actors {
		if w, ok := byPod[a.WorkerPod]; ok && a.WorkerPod != "" {
			w.Actors = append(w.Actors, a.Atespace+"/"+a.Name)
		}
	}
	sort.Slice(workers, func(i, j int) bool {
		if workers[i].Namespace != workers[j].Namespace {
			return workers[i].Namespace < workers[j].Namespace
		}
		return workers[i].Pod < workers[j].Pod
	})
	sort.Slice(actors, func(i, j int) bool {
		if actors[i].Atespace != actors[j].Atespace {
			return actors[i].Atespace < actors[j].Atespace
		}
		return actors[i].Name < actors[j].Name
	})
	slog.Debug("polled ateapi", "workers", len(workers), "actors", len(actors))
	return workers, actors, nil
}

func trimEnum(s, prefix string) string { return strings.ToLower(strings.TrimPrefix(s, prefix)) }

func joinNonEmpty(ns, name string) string {
	if name == "" {
		return ""
	}
	return ns + "/" + name
}
