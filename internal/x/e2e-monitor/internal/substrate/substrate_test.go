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

package substrate

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

type fakeLister struct {
	fail bool
}

func (f *fakeLister) ListWorkers(_ context.Context, in *ateapipb.ListWorkersRequest, _ ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error) {
	if f.fail {
		return nil, errors.New("unavailable")
	}
	// Two pages.
	if in.GetOptions().GetPageToken() == "" {
		return &ateapipb.ListWorkersResponse{
			Workers: []*ateapipb.Worker{{
				Metadata:        &ateapipb.ResourceMetadata{Name: "w1"},
				WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "pod-1",
				Status: &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE},
			}},
			NextPageToken: "next",
		}, nil
	}
	return &ateapipb.ListWorkersResponse{Workers: []*ateapipb.Worker{{
		Metadata: &ateapipb.ResourceMetadata{Name: "w2"}, WorkerNamespace: "ns", WorkerPod: "pod-2",
	}}}, nil
}

func (f *fakeLister) ListActors(context.Context, *ateapipb.ListActorsRequest, ...grpc.CallOption) (*ateapipb.ListActorsResponse, error) {
	return &ateapipb.ListActorsResponse{Actors: []*ateapipb.Actor{{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "demo", Name: "counter", Uid: "u1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "demo", Name: "tmpl"},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerNamespace: "ns", WorkerPod: "pod-1"},
		},
	}}}, nil
}

func TestPoll(t *testing.T) {
	workers, actors, err := poll(context.Background(), &fakeLister{})
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 2 || workers[0].State != "active" || len(workers[0].Actors) != 1 || workers[0].Actors[0] != "demo/counter" {
		t.Errorf("workers = %+v", workers)
	}
	if len(actors) != 1 || actors[0].State != "running" || actors[0].WorkerPod != "ns/pod-1" || actors[0].Template != "tmpl" {
		t.Errorf("actors = %+v", actors)
	}
}

func TestPollerRun(t *testing.T) {
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/statusz" || r.URL.Query().Get("format") != "json" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(`{"mode":"envoy"}`))
	}))
	defer router.Close()

	lister := &fakeLister{fail: true}
	dials := 0
	p := NewPoller(
		func(context.Context) (Lister, func(), error) { dials++; return lister, func() {}, nil },
		func(context.Context) (string, func(), error) { return router.URL, func() {}, nil },
		10*time.Millisecond,
	)
	ch, stop := p.Changes()
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	<-ch
	s := p.State()
	if s.APIError == "" || string(s.Router) != `{"mode":"envoy"}` {
		t.Errorf("first poll = %+v", s)
	}
	<-ch
	if dials < 2 {
		t.Errorf("poller did not redial after an API error (dials = %d)", dials)
	}
}
