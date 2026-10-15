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

// Package monitortest builds a Monitor over a fake cluster for frontend tests.
package monitortest

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/agent-substrate/substrate/internal/e2e/e2etrack"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/discovery"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/e2efeed"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/monitor"
)

// Sources of the fake cluster's containers.
var (
	Controller = logs.Source{Namespace: "ate-system", Pod: "ate-controller-1", Container: "main"}
	// Worker1 is in TestNamespace, Worker2 in another e2e namespace.
	Worker1 = logs.Source{Namespace: TestNamespace, Pod: "counter-1", Container: "main"}
	Worker2 = logs.Source{Namespace: "zzzz-999", Pod: "counter-2", Container: "main"}
)

// TestNamespace is the namespace StartTest tracks.
const TestNamespace = "abcd-123"

// Fixture is a Monitor over a fake cluster: an ate-controller Deployment with
// one pod, and two e2e namespaces with one worker pod each.
type Fixture struct {
	Monitor *monitor.Monitor
	Logs    *logs.Manager
	Feed    *e2efeed.Feed
}

func runningPod(ns, name string, lbls map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: lbls},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "main", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
		},
	}
}

// New builds a Fixture; everything stops when the test ends.
func New(t testing.TB, follow monitor.FollowMode) *Fixture {
	t.Helper()
	one := int32(1)
	objs := []runtime.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "ate-controller"},
			Spec: appsv1.DeploymentSpec{
				Replicas: &one,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "ate-controller"}},
			},
			Status: appsv1.DeploymentStatus{ReadyReplicas: 1},
		},
		runningPod("ate-system", Controller.Pod, map[string]string{"app": "ate-controller"}),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: TestNamespace, Labels: map[string]string{discovery.E2ENamespaceLabel: "true"}}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: Worker2.Namespace, Labels: map[string]string{discovery.E2ENamespaceLabel: "true"}}},
		runningPod(Worker1.Namespace, Worker1.Pod, map[string]string{discovery.WorkerPoolLabel: "counter"}),
		runningPod(Worker2.Namespace, Worker2.Pod, map[string]string{discovery.WorkerPoolLabel: "counter"}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	disc := discovery.New(discovery.Config{Kube: k8sfake.NewClientset(objs...), Namespaces: []string{"ate-system"}})
	if err := disc.Start(ctx, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	opener := func(context.Context, logs.Source, *corev1.PodLogOptions) (io.ReadCloser, error) {
		return nil, errors.New("no log streams in tests")
	}
	lm := logs.NewManager(opener, logs.Options{})
	t.Cleanup(lm.Close)
	feed := e2efeed.New()
	m := monitor.New(ctx, monitor.Config{
		Discovery:     disc,
		Logs:          lm,
		Feed:          feed,
		FollowWorkers: follow,
		Info:          monitor.Info{Context: "fake", SystemNamespace: "ate-system"},
	})
	return &Fixture{Monitor: m, Logs: lm, Feed: feed}
}

// TestID is the ID of the test StartTest starts.
const TestID = "p.TestA"

// StartTest starts p.TestA at the given time, tracking refs.
func (f *Fixture) StartTest(at time.Time, refs ...e2etrack.Ref) {
	f.Feed.Apply(e2efeed.Event{Time: at, Action: "run", Package: "p", Test: "TestA"})
	f.Feed.Apply(e2efeed.Event{Time: at, Action: "output", Package: "p", Test: "TestA", Output: "    a_test.go:10: starting\n"})
	for _, r := range refs {
		f.Feed.Apply(e2efeed.Event{Time: at, Action: "output", Package: "p", Test: "TestA", Output: "    a_test.go:11: " + e2etrack.Format(r) + "\n"})
	}
}

// EndTest ends p.TestA with the given action ("pass", "fail").
func (f *Fixture) EndTest(at time.Time, action string) {
	f.Feed.Apply(e2efeed.Event{Time: at, Action: action, Package: "p", Test: "TestA", Elapsed: 1})
}
