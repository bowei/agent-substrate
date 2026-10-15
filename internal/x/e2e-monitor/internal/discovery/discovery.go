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

// Package discovery watches the cluster and builds the dashboard's model of
// substrate's components: every workload in the watched namespaces, and every
// WorkerPool with its worker pods.
package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	substrateinformers "github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	substratelisters "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
)

const (
	// WorkerPoolLabel is set by atecontroller on every worker pod.
	WorkerPoolLabel = "ate.dev/worker-pool"
	// E2ENamespaceLabel marks namespaces internal/e2e creates.
	E2ENamespaceLabel = "ate.dev/e2e"
	// E2ETestAnnotation names the test that created an e2e namespace.
	E2ETestAnnotation = "ate.dev/e2e-test"
	// e2eFixturePrefix starts the namespaces of e2e fixture pools.
	e2eFixturePrefix = "ate-e2e"
)

// Config configures a Discoverer.
type Config struct {
	Kube kubernetes.Interface
	// Substrate lists WorkerPools; nil derives pools from worker pod labels
	// alone.
	Substrate versioned.Interface
	// Namespaces are watched for components; the first is substrate's system
	// namespace.
	Namespaces []string
	// ResourcePrefix is stripped from workload names before looking up
	// known-component metadata.
	ResourcePrefix string
}

type nsListers struct {
	deployments  appslisters.DeploymentLister
	daemonSets   appslisters.DaemonSetLister
	statefulSets appslisters.StatefulSetLister
	pods         corelisters.PodLister
	events       corelisters.EventLister
}

// Discoverer maintains informers and builds snapshots from their caches.
type Discoverer struct {
	cfg Config

	factories    []informers.SharedInformerFactory
	nsListers    map[string]nsListers
	workerPods   corelisters.PodLister
	namespaces   corelisters.NamespaceLister
	subFactory   substrateinformers.SharedInformerFactory
	workerPools  substratelisters.WorkerPoolLister
	synced       []cache.InformerSynced
	subsMu       sync.Mutex
	subscribers  map[chan struct{}]struct{}
	poolsEnabled bool
}

// New builds a Discoverer; call Start before reading from it.
func New(cfg Config) *Discoverer {
	d := &Discoverer{
		cfg:         cfg,
		nsListers:   map[string]nsListers{},
		subscribers: map[chan struct{}]struct{}{},
	}
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { d.signal() },
		UpdateFunc: func(any, any) { d.signal() },
		DeleteFunc: func(any) { d.signal() },
	}
	register := func(inf cache.SharedIndexInformer) {
		if _, err := inf.AddEventHandler(handler); err != nil {
			slog.Error("adding event handler", "err", err)
		}
		d.synced = append(d.synced, inf.HasSynced)
	}

	for _, ns := range cfg.Namespaces {
		if _, ok := d.nsListers[ns]; ok {
			continue
		}
		f := informers.NewSharedInformerFactoryWithOptions(cfg.Kube, 0, informers.WithNamespace(ns))
		l := nsListers{
			deployments:  f.Apps().V1().Deployments().Lister(),
			daemonSets:   f.Apps().V1().DaemonSets().Lister(),
			statefulSets: f.Apps().V1().StatefulSets().Lister(),
			pods:         f.Core().V1().Pods().Lister(),
			events:       f.Core().V1().Events().Lister(),
		}
		register(f.Apps().V1().Deployments().Informer())
		register(f.Apps().V1().DaemonSets().Informer())
		register(f.Apps().V1().StatefulSets().Informer())
		register(f.Core().V1().Pods().Informer())
		register(f.Core().V1().Events().Informer())
		d.nsListers[ns] = l
		d.factories = append(d.factories, f)
	}

	wf := informers.NewSharedInformerFactoryWithOptions(cfg.Kube, 0, informers.WithTweakListOptions(func(o *metav1.ListOptions) {
		o.LabelSelector = WorkerPoolLabel
	}))
	d.workerPods = wf.Core().V1().Pods().Lister()
	register(wf.Core().V1().Pods().Informer())
	d.factories = append(d.factories, wf)

	nf := informers.NewSharedInformerFactory(cfg.Kube, 0)
	d.namespaces = nf.Core().V1().Namespaces().Lister()
	register(nf.Core().V1().Namespaces().Informer())
	d.factories = append(d.factories, nf)
	return d
}

// Start starts the informers, which run until ctx ends, and waits up to
// syncTimeout for their caches to fill.
func (d *Discoverer) Start(ctx context.Context, syncTimeout time.Duration) error {
	if d.cfg.Substrate != nil {
		// Without the CRD the WorkerPool informer would never sync, so probe
		// for it first.
		probeCtx, cancel := context.WithTimeout(ctx, syncTimeout)
		_, err := d.cfg.Substrate.ApiV1alpha1().WorkerPools("").List(probeCtx, metav1.ListOptions{Limit: 1})
		cancel()
		if err != nil {
			slog.Warn("WorkerPools unavailable; deriving pools from worker pod labels", "err", err)
		} else {
			d.subFactory = substrateinformers.NewSharedInformerFactory(d.cfg.Substrate, 0)
			inf := d.subFactory.Api().V1alpha1().WorkerPools()
			d.workerPools = inf.Lister()
			if _, err := inf.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc:    func(any) { d.signal() },
				UpdateFunc: func(any, any) { d.signal() },
				DeleteFunc: func(any) { d.signal() },
			}); err != nil {
				return err
			}
			d.synced = append(d.synced, inf.Informer().HasSynced)
			d.subFactory.Start(ctx.Done())
			d.poolsEnabled = true
		}
	}
	for _, f := range d.factories {
		f.Start(ctx.Done())
	}
	waitCtx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()
	if !cache.WaitForCacheSync(waitCtx.Done(), d.synced...) {
		return fmt.Errorf("informer caches did not sync within %v", syncTimeout)
	}
	return nil
}

// Changes returns a channel that receives (coalesced) notifications whenever
// a watched object changes, and a func to stop them.
func (d *Discoverer) Changes() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	d.subsMu.Lock()
	d.subscribers[ch] = struct{}{}
	d.subsMu.Unlock()
	return ch, func() {
		d.subsMu.Lock()
		delete(d.subscribers, ch)
		d.subsMu.Unlock()
	}
}

func (d *Discoverer) signal() {
	d.subsMu.Lock()
	defer d.subsMu.Unlock()
	for ch := range d.subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Components returns every component in the watched namespaces, known
// components first.
func (d *Discoverer) Components() []Component {
	var out []Component
	for _, ns := range d.cfg.Namespaces {
		l, ok := d.nsListers[ns]
		if !ok {
			continue
		}
		out = append(out, d.namespaceComponents(ns, l)...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].order != out[j].order {
			return out[i].order < out[j].order
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// Component returns the named component.
func (d *Discoverer) Component(name string) (Component, bool) {
	for _, c := range d.Components() {
		if c.Name == name {
			return c, true
		}
	}
	return Component{}, false
}

type workload struct {
	kind     string
	name     string
	selector *metav1.LabelSelector
	desired  int32
	ready    int32
	podSpec  corev1.PodSpec
}

func (d *Discoverer) namespaceComponents(ns string, l nsListers) []Component {
	var wls []workload
	if deps, err := l.deployments.List(labels.Everything()); err == nil {
		for _, o := range deps {
			wls = append(wls, workload{kind: "Deployment", name: o.Name, selector: o.Spec.Selector, desired: replicas(o.Spec.Replicas), ready: o.Status.ReadyReplicas, podSpec: o.Spec.Template.Spec})
		}
	}
	if dss, err := l.daemonSets.List(labels.Everything()); err == nil {
		for _, o := range dss {
			wls = append(wls, workload{kind: "DaemonSet", name: o.Name, selector: o.Spec.Selector, desired: o.Status.DesiredNumberScheduled, ready: o.Status.NumberReady, podSpec: o.Spec.Template.Spec})
		}
	}
	if stss, err := l.statefulSets.List(labels.Everything()); err == nil {
		for _, o := range stss {
			wls = append(wls, workload{kind: "StatefulSet", name: o.Name, selector: o.Spec.Selector, desired: replicas(o.Spec.Replicas), ready: o.Status.ReadyReplicas, podSpec: o.Spec.Template.Spec})
		}
	}
	pods, _ := l.pods.List(labels.Everything())
	events, _ := l.events.List(labels.Everything())

	claimed := map[string]bool{}
	var out []Component
	for _, w := range wls {
		c := Component{
			Name:      w.name,
			Namespace: ns,
			Kind:      w.kind,
			Desired:   w.desired,
			Ready:     w.ready,
			Health:    health(w.desired, w.ready),
			Images:    images(w.podSpec),
		}
		info := lookupKnown(strings.TrimPrefix(w.name, d.cfg.ResourcePrefix))
		c.Description, c.StatuszPort, c.order = info.description, info.statuszPort, info.order
		sel, err := metav1.LabelSelectorAsSelector(w.selector)
		if err == nil && !sel.Empty() {
			for _, p := range pods {
				if sel.Matches(labels.Set(p.Labels)) {
					c.Pods = append(c.Pods, convertPod(p))
					claimed[p.Name] = true
				}
			}
		}
		sortPods(c.Pods)
		c.Events = componentEvents(events, w.name, c.Pods)
		out = append(out, c)
	}

	// Pods no workload selects (Jobs, bare pods) are still worth seeing.
	var rest []Pod
	for _, p := range pods {
		if !claimed[p.Name] {
			rest = append(rest, convertPod(p))
		}
	}
	if len(rest) > 0 {
		sortPods(rest)
		c := Component{
			Name:        "other-pods." + ns,
			Namespace:   ns,
			Kind:        "Pods",
			Description: "Pods in " + ns + " that no Deployment, DaemonSet, or StatefulSet selects.",
			Pods:        rest,
			Health:      HealthUnknown,
			order:       otherOrder,
		}
		var running int32
		for _, p := range rest {
			if p.Phase == string(corev1.PodRunning) || p.Phase == string(corev1.PodSucceeded) {
				running++
			}
		}
		c.Desired, c.Ready = int32(len(rest)), running
		out = append(out, c)
	}
	return out
}

func replicas(r *int32) int32 {
	if r == nil {
		return 1
	}
	return *r
}

func images(spec corev1.PodSpec) []string {
	var out []string
	for _, c := range spec.Containers {
		out = append(out, c.Image)
	}
	return out
}

func sortPods(ps []Pod) {
	sort.Slice(ps, func(i, j int) bool {
		if ps[i].Namespace != ps[j].Namespace {
			return ps[i].Namespace < ps[j].Namespace
		}
		return ps[i].Name < ps[j].Name
	})
}

const maxEvents = 30

// componentEvents returns the most recent events about the workload, the
// objects it owns (named after it), or its pods.
func componentEvents(events []*corev1.Event, name string, pods []Pod) []Event {
	podNames := map[string]bool{}
	for _, p := range pods {
		podNames[p.Name] = true
	}
	var out []Event
	for _, e := range events {
		n := e.InvolvedObject.Name
		if n == name || strings.HasPrefix(n, name+"-") || podNames[n] {
			out = append(out, convertEvent(e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time.After(out[j].Time) })
	if len(out) > maxEvents {
		out = out[:maxEvents]
	}
	return out
}

// Namespaces returns the namespaces that belong to e2e tests: those labeled
// by internal/e2e and those holding e2e fixture pools.
func (d *Discoverer) Namespaces() []Namespace {
	nss, err := d.namespaces.List(labels.Everything())
	if err != nil {
		return nil
	}
	var out []Namespace
	for _, ns := range nss {
		if n := namespaceInfo(ns); n.E2E {
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func namespaceInfo(ns *corev1.Namespace) Namespace {
	return Namespace{
		Name: ns.Name,
		E2E:  ns.Labels[E2ENamespaceLabel] == "true" || strings.HasPrefix(ns.Name, e2eFixturePrefix),
		Test: ns.Annotations[E2ETestAnnotation],
	}
}

// Pools returns every WorkerPool (or, without the CRD, every pool named by a
// worker pod label) with its worker pods.
func (d *Discoverer) Pools() []Pool {
	type key struct{ ns, name string }
	pools := map[key]*Pool{}
	get := func(ns, name string) *Pool {
		k := key{ns, name}
		p, ok := pools[k]
		if !ok {
			p = &Pool{Namespace: ns, Name: name}
			if n, err := d.namespaces.Get(ns); err == nil {
				info := namespaceInfo(n)
				p.E2E, p.Test = info.E2E, info.Test
			}
			pools[k] = p
		}
		return p
	}
	if d.poolsEnabled {
		if wps, err := d.workerPools.List(labels.Everything()); err == nil {
			for _, wp := range wps {
				p := get(wp.Namespace, wp.Name)
				p.Replicas = wp.Spec.Replicas
				p.Ready = wp.Status.ReadyReplicas
				p.SandboxClass = string(wp.Spec.DefaultSandboxClass())
				p.Image = wp.Spec.WorkerImage
			}
		}
	}
	if pods, err := d.workerPods.List(labels.Everything()); err == nil {
		for _, pod := range pods {
			p := get(pod.Namespace, pod.Labels[WorkerPoolLabel])
			p.Pods = append(p.Pods, convertPod(pod))
		}
	}
	out := make([]Pool, 0, len(pools))
	for _, p := range pools {
		sortPods(p.Pods)
		if !d.poolsEnabled {
			p.Replicas = int32(len(p.Pods))
			for _, pod := range p.Pods {
				if pod.Phase == string(corev1.PodRunning) && !strings.HasPrefix(pod.Ready, "0/") {
					p.Ready++
				}
			}
		}
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].E2E != out[j].E2E {
			return !out[i].E2E
		}
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}
