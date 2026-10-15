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

package discovery

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	substratefake "github.com/agent-substrate/substrate/pkg/client/clientset/versioned/fake"
)

func ptr[T any](v T) *T { return &v }

func deployment(ns, name string, replicas, ready int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: name + ":dev"}}}},
		},
		Status: appsv1.DeploymentStatus{ReadyReplicas: ready},
	}
}

func pod(ns, name string, lbls map[string]string, ready bool) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: lbls},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{Name: "setup"},
				{Name: "otel", RestartPolicy: ptr(corev1.ContainerRestartPolicyAlways)},
			},
			Containers: []corev1.Container{{Name: "main"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			InitContainerStatuses: []corev1.ContainerStatus{
				{Name: "otel", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:         "main",
				Ready:        ready,
				RestartCount: 2,
				State:        corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			}},
		},
	}
}

func start(t *testing.T, kubeObjs []runtime.Object, subObjs []runtime.Object) *Discoverer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	d := New(Config{
		Kube:       k8sfake.NewClientset(kubeObjs...),
		Substrate:  substratefake.NewSimpleClientset(subObjs...),
		Namespaces: []string{"ate-system"},
	})
	if err := d.Start(ctx, 30*time.Second); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return d
}

func TestComponents(t *testing.T) {
	d := start(t, []runtime.Object{
		deployment("ate-system", "ate-api-server", 2, 1),
		deployment("ate-system", "brand-new-thing", 1, 1),
		pod("ate-system", "ate-api-server-a", map[string]string{"app": "ate-api-server"}, false),
		pod("ate-system", "ate-api-server-b", map[string]string{"app": "ate-api-server"}, true),
		pod("ate-system", "job-pod", map[string]string{"job": "x"}, true),
		pod("elsewhere", "ignored", map[string]string{"app": "ate-api-server"}, true),
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Namespace: "ate-system", Name: "e1"},
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "ate-api-server-a"},
			Type:           "Warning", Reason: "BackOff", LastTimestamp: metav1.NewTime(time.Now()),
		},
	}, nil)

	cs := d.Components()
	var names []string
	for _, c := range cs {
		names = append(names, c.Name)
	}
	want := []string{"ate-api-server", "brand-new-thing", "other-pods.ate-system"}
	if len(names) != len(want) {
		t.Fatalf("components = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("components = %v, want %v", names, want)
		}
	}

	api := cs[0]
	if api.Health != HealthDegraded || len(api.Pods) != 2 || api.Description == "" {
		t.Errorf("ate-api-server = %+v", api)
	}
	if len(api.Events) != 1 || api.Events[0].Reason != "BackOff" {
		t.Errorf("events = %+v", api.Events)
	}
	p := api.Pods[0]
	if p.Ready != "1/2" || p.Restarts != 2 || p.Reason != "CrashLoopBackOff" {
		t.Errorf("pod = %+v", p)
	}
	var longRunning []string
	for _, c := range p.Containers {
		if c.LongRunning() {
			longRunning = append(longRunning, c.Name)
		}
	}
	if len(longRunning) != 2 || longRunning[0] != "otel" || longRunning[1] != "main" {
		t.Errorf("long-running containers = %v, want [otel main]", longRunning)
	}
	if cs[1].Description != "" || cs[1].Health != HealthOK {
		t.Errorf("unknown component = %+v", cs[1])
	}
}

func TestPools(t *testing.T) {
	d := start(t, []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name:        "abcd-123",
			Labels:      map[string]string{E2ENamespaceLabel: "true"},
			Annotations: map[string]string{E2ETestAnnotation: "TestDemo/sub"},
		}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ate-demo-counter"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ate-e2e-probe-x"}},
		pod("abcd-123", "counter-1", map[string]string{WorkerPoolLabel: "counter"}, true),
		pod("ate-demo-counter", "counter-2", map[string]string{WorkerPoolLabel: "counter"}, true),
		pod("ate-system", "not-a-worker", map[string]string{"app": "x"}, true),
	}, []runtime.Object{
		&v1alpha1.WorkerPool{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ate-demo-counter", Name: "counter"},
			Spec:       v1alpha1.WorkerPoolSpec{Replicas: 3, WorkerImage: "worker:dev"},
			Status:     v1alpha1.WorkerPoolStatus{ReadyReplicas: 1},
		},
	})

	pools := d.Pools()
	if len(pools) != 2 {
		t.Fatalf("pools = %+v", pools)
	}
	demo, e2e := pools[0], pools[1]
	if demo.Namespace != "ate-demo-counter" || demo.E2E || demo.Replicas != 3 || demo.Ready != 1 || len(demo.Pods) != 1 {
		t.Errorf("demo pool = %+v", demo)
	}
	if e2e.Namespace != "abcd-123" || !e2e.E2E || e2e.Test != "TestDemo/sub" || len(e2e.Pods) != 1 || e2e.Pods[0].Pool != "counter" {
		t.Errorf("e2e pool = %+v", e2e)
	}

	nss := d.Namespaces()
	if len(nss) != 2 || nss[0].Name != "abcd-123" || nss[1].Name != "ate-e2e-probe-x" {
		t.Errorf("e2e namespaces = %+v", nss)
	}
}

func TestChangesNotify(t *testing.T) {
	kube := k8sfake.NewClientset()
	d := New(Config{Kube: kube, Namespaces: []string{"ate-system"}})
	ch, stop := d.Changes()
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.Start(ctx, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := kube.AppsV1().Deployments("ate-system").Create(ctx, deployment("ate-system", "atelet", 1, 1), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-ch:
			if _, ok := d.Component("atelet"); ok {
				return
			}
		case <-deadline:
			t.Fatal("no change notification for a new Deployment")
		}
	}
}
