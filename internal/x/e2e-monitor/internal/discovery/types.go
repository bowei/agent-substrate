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
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Health summarizes a component's replicas.
type Health string

const (
	HealthOK       Health = "ok"
	HealthDegraded Health = "degraded"
	HealthDown     Health = "down"
	HealthUnknown  Health = "unknown"
)

// Container is one container of a pod.
type Container struct {
	Name string `json:"name"`
	// Init is set for init containers. Sidecar marks a restartable (native
	// sidecar) init container, which runs for the life of the pod.
	Init     bool   `json:"init,omitempty"`
	Sidecar  bool   `json:"sidecar,omitempty"`
	Image    string `json:"image"`
	Ready    bool   `json:"ready"`
	Restarts int32  `json:"restarts"`
	// State is running, waiting, terminated, or empty before the kubelet
	// reports it; Reason qualifies waiting and terminated.
	State  string `json:"state,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// LongRunning reports whether the container runs for the life of the pod.
func (c Container) LongRunning() bool { return !c.Init || c.Sidecar }

// Pod is the dashboard's view of a pod.
type Pod struct {
	Namespace  string      `json:"namespace"`
	Name       string      `json:"name"`
	Phase      string      `json:"phase"`
	Ready      string      `json:"ready"`
	Restarts   int32       `json:"restarts"`
	Node       string      `json:"node,omitempty"`
	IP         string      `json:"ip,omitempty"`
	Reason     string      `json:"reason,omitempty"`
	Created    time.Time   `json:"created"`
	Containers []Container `json:"containers"`
	// Pool is the WorkerPool of a worker pod.
	Pool string `json:"pool,omitempty"`
}

// Event is a Kubernetes event about a component.
type Event struct {
	Time    time.Time `json:"time"`
	Type    string    `json:"type"`
	Reason  string    `json:"reason"`
	Object  string    `json:"object"`
	Message string    `json:"message"`
	Count   int32     `json:"count"`
}

// Component is one substrate workload: a Deployment, DaemonSet, or
// StatefulSet, or the leftover pods of a namespace no workload selects.
type Component struct {
	Name        string   `json:"name"`
	Namespace   string   `json:"namespace"`
	Kind        string   `json:"kind"`
	Description string   `json:"description,omitempty"`
	Desired     int32    `json:"desired"`
	Ready       int32    `json:"ready"`
	Health      Health   `json:"health"`
	Images      []string `json:"images,omitempty"`
	Pods        []Pod    `json:"pods"`
	Events      []Event  `json:"events,omitempty"`
	// StatuszPort is the container port serving /statusz, when known.
	StatuszPort int `json:"statuszPort,omitempty"`
	order       int
}

// Pool is a WorkerPool and its worker pods.
type Pool struct {
	Namespace    string `json:"namespace"`
	Name         string `json:"name"`
	Replicas     int32  `json:"replicas"`
	Ready        int32  `json:"ready"`
	SandboxClass string `json:"sandboxClass,omitempty"`
	Image        string `json:"image,omitempty"`
	// E2E is set when the pool's namespace belongs to an e2e test; Test
	// names it when known.
	E2E  bool   `json:"e2e"`
	Test string `json:"test,omitempty"`
	Pods []Pod  `json:"pods"`
}

// Namespace is what discovery knows about a namespace's e2e ownership.
type Namespace struct {
	Name string `json:"name"`
	E2E  bool   `json:"e2e"`
	Test string `json:"test,omitempty"`
}

func health(desired, ready int32) Health {
	switch {
	case desired == 0:
		return HealthUnknown
	case ready >= desired:
		return HealthOK
	case ready == 0:
		return HealthDown
	default:
		return HealthDegraded
	}
}

func convertPod(p *corev1.Pod) Pod {
	out := Pod{
		Namespace: p.Namespace,
		Name:      p.Name,
		Phase:     string(p.Status.Phase),
		Node:      p.Spec.NodeName,
		IP:        p.Status.PodIP,
		Reason:    p.Status.Reason,
		Created:   p.CreationTimestamp.Time,
		Pool:      p.Labels[WorkerPoolLabel],
	}
	statuses := map[string]corev1.ContainerStatus{}
	for _, cs := range p.Status.InitContainerStatuses {
		statuses["init/"+cs.Name] = cs
	}
	for _, cs := range p.Status.ContainerStatuses {
		statuses[cs.Name] = cs
	}
	add := func(c corev1.Container, init bool) {
		key := c.Name
		if init {
			key = "init/" + c.Name
		}
		cont := Container{
			Name:    c.Name,
			Init:    init,
			Sidecar: init && c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways,
			Image:   c.Image,
		}
		if cs, ok := statuses[key]; ok {
			cont.Ready = cs.Ready
			cont.Restarts = cs.RestartCount
			switch {
			case cs.State.Running != nil:
				cont.State = "running"
			case cs.State.Waiting != nil:
				cont.State, cont.Reason = "waiting", cs.State.Waiting.Reason
			case cs.State.Terminated != nil:
				cont.State, cont.Reason = "terminated", cs.State.Terminated.Reason
			}
		}
		out.Containers = append(out.Containers, cont)
	}
	for _, c := range p.Spec.InitContainers {
		add(c, true)
	}
	for _, c := range p.Spec.Containers {
		add(c, false)
	}

	var ready, total int
	for _, c := range out.Containers {
		if !c.LongRunning() {
			continue
		}
		total++
		out.Restarts += c.Restarts
		if c.Ready {
			ready++
		}
		// The most telling container reason wins over the pod's.
		if out.Reason == "" && c.State == "waiting" && c.Reason != "" && c.Reason != "ContainerCreating" {
			out.Reason = c.Reason
		}
	}
	out.Ready = strconv.Itoa(ready) + "/" + strconv.Itoa(total)
	if p.DeletionTimestamp != nil {
		out.Reason = "Terminating"
	}
	return out
}

func convertEvent(e *corev1.Event) Event {
	t := e.LastTimestamp.Time
	if t.IsZero() {
		t = e.EventTime.Time
	}
	if t.IsZero() {
		t = e.CreationTimestamp.Time
	}
	count := e.Count
	if e.Series != nil {
		count = e.Series.Count
	}
	return Event{
		Time:    t,
		Type:    e.Type,
		Reason:  e.Reason,
		Object:  strings.ToLower(e.InvolvedObject.Kind) + "/" + e.InvolvedObject.Name,
		Message: e.Message,
		Count:   count,
	}
}
