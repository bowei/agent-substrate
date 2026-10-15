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

package logs

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Options configures a Manager.
type Options struct {
	// TailLines is how many existing lines a new stream starts with.
	TailLines int64
	// Capacity is the ring buffer size per container.
	Capacity int
	// Retain is how long the lines of a stopped stream are kept, so the tail
	// of a deleted pod stays readable.
	Retain time.Duration
	// PinTTL is how long an on-demand stream (see Touch) keeps following
	// after it was last asked for.
	PinTTL time.Duration
}

func (o *Options) defaults() {
	if o.TailLines <= 0 {
		o.TailLines = 500
	}
	if o.Capacity <= 0 {
		o.Capacity = 5000
	}
	if o.Retain <= 0 {
		o.Retain = 30 * time.Minute
	}
	if o.PinTTL <= 0 {
		o.PinTTL = 5 * time.Minute
	}
}

// Opener opens a container log stream.
type Opener func(ctx context.Context, src Source, opts *corev1.PodLogOptions) (io.ReadCloser, error)

// KubeOpener opens log streams through the Kubernetes API.
func KubeOpener(client kubernetes.Interface) Opener {
	return func(ctx context.Context, src Source, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
		return client.CoreV1().Pods(src.Namespace).GetLogs(src.Pod, opts).Stream(ctx)
	}
}

// StreamInfo describes one followed (or recently followed) container.
type StreamInfo struct {
	Source
	Following bool      `json:"following"`
	Pinned    bool      `json:"pinned"`
	Lines     int       `json:"lines"`
	Error     string    `json:"error,omitempty"`
	StoppedAt time.Time `json:"stoppedAt,omitzero"`
}

type stream struct {
	src         Source
	ring        *ring
	cancel      context.CancelFunc
	stoppedAt   time.Time
	pinnedUntil time.Time
	lastKube    time.Time
	err         string
}

type subscriber struct {
	f  Filter
	ch chan Line
}

// Manager owns every log stream.
type Manager struct {
	opts Options
	open Opener
	now  func() time.Time

	mu      sync.Mutex
	seq     uint64
	streams map[Source]*stream
	subs    map[*subscriber]struct{}
}

// NewManager returns a Manager that opens streams with open.
func NewManager(open Opener, opts Options) *Manager {
	opts.defaults()
	return &Manager{
		opts:    opts,
		open:    open,
		now:     time.Now,
		streams: map[Source]*stream{},
		subs:    map[*subscriber]struct{}{},
	}
}

// Follow starts following src if it is not already followed.
func (m *Manager) Follow(ctx context.Context, src Source) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.followLocked(ctx, src)
}

func (m *Manager) followLocked(ctx context.Context, src Source) *stream {
	st, ok := m.streams[src]
	if !ok {
		st = &stream{src: src, ring: newRing(m.opts.Capacity)}
		m.streams[src] = st
	}
	if st.cancel == nil {
		sctx, cancel := context.WithCancel(ctx)
		st.cancel = cancel
		st.stoppedAt = time.Time{}
		go m.run(sctx, st)
	}
	return st
}

// Touch follows src on demand: it keeps following for PinTTL after the last
// Touch even if Sync does not want it.
func (m *Manager) Touch(ctx context.Context, src Source) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.followLocked(ctx, src)
	st.pinnedUntil = m.now().Add(m.opts.PinTTL)
}

// Sync follows every source in desired and stops every other unpinned one.
func (m *Manager) Sync(ctx context.Context, desired map[Source]bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for src := range desired {
		m.followLocked(ctx, src)
	}
	now := m.now()
	for src, st := range m.streams {
		if !desired[src] && st.pinnedUntil.Before(now) {
			m.stopLocked(st)
		}
	}
}

func (m *Manager) stopLocked(st *stream) {
	if st.cancel != nil {
		st.cancel()
		st.cancel = nil
		st.stoppedAt = m.now()
	}
}

// GC drops the lines of streams stopped longer than Retain ago.
func (m *Manager) GC() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for src, st := range m.streams {
		if st.cancel == nil && !st.stoppedAt.IsZero() && now.Sub(st.stoppedAt) > m.opts.Retain {
			delete(m.streams, src)
		}
	}
}

// Close stops every stream.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, st := range m.streams {
		m.stopLocked(st)
	}
}

// Streams lists the known streams.
func (m *Manager) Streams() []StreamInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	out := make([]StreamInfo, 0, len(m.streams))
	for _, st := range m.streams {
		out = append(out, StreamInfo{
			Source:    st.src,
			Following: st.cancel != nil,
			Pinned:    st.pinnedUntil.After(now),
			Lines:     st.ring.n,
			Error:     st.err,
			StoppedAt: st.stoppedAt,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

func (m *Manager) run(ctx context.Context, st *stream) {
	const maxBackoff = 30 * time.Second
	backoff := time.Second
	for ctx.Err() == nil {
		m.mu.Lock()
		last := st.lastKube
		m.mu.Unlock()

		opts := &corev1.PodLogOptions{Container: st.src.Container, Follow: true, Timestamps: true}
		if last.IsZero() {
			tail := m.opts.TailLines
			opts.TailLines = &tail
		} else {
			// SinceTime has second precision; lines at or before last are
			// dropped below.
			opts.SinceTime = &metav1.Time{Time: last}
		}
		rc, err := m.open(ctx, st.src, opts)
		if err != nil {
			m.setErr(st, err.Error())
		} else {
			m.setErr(st, "")
			if m.consume(st, rc) > 0 {
				backoff = time.Second
			}
			rc.Close()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// consume reads rc until it ends and returns how many lines it appended.
func (m *Manager) consume(st *stream, rc io.Reader) int {
	sc := bufio.NewScanner(rc)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	n := 0
	for sc.Scan() {
		l := parseLine(st.src, sc.Text())
		m.mu.Lock()
		dup := !l.kubeTime.IsZero() && !st.lastKube.IsZero() && !l.kubeTime.After(st.lastKube)
		if !dup {
			if !l.kubeTime.IsZero() {
				st.lastKube = l.kubeTime
			}
			m.appendLocked(st, l)
			n++
		}
		m.mu.Unlock()
	}
	if err := sc.Err(); err != nil {
		slog.Debug("log stream ended", "source", st.src.String(), "err", err)
	}
	return n
}

func (m *Manager) setErr(st *stream, msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st.err = msg
}

func (m *Manager) appendLocked(st *stream, l Line) {
	m.seq++
	l.Seq = m.seq
	st.ring.add(l)
	for s := range m.subs {
		if !s.f.Matches(l) {
			continue
		}
		select {
		case s.ch <- l:
		default:
			// A slow subscriber misses lines rather than stalling every stream.
		}
	}
}

// Append adds a line to src's buffer as if it had been read from the stream.
// It is meant for tests and for replaying recorded logs.
func (m *Manager) Append(src Source, text string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.streams[src]
	if !ok {
		st = &stream{src: src, ring: newRing(m.opts.Capacity)}
		m.streams[src] = st
	}
	m.appendLocked(st, parseLine(src, text))
}

// Query returns the last limit lines matching f across every buffer, oldest
// first.
func (m *Manager) Query(f Filter, limit int) []Line {
	m.mu.Lock()
	var out []Line
	for src, st := range m.streams {
		if !f.wantsSource(src) {
			continue
		}
		st.ring.each(func(l Line) {
			if f.Matches(l) {
				out = append(out, l)
			}
		})
	}
	m.mu.Unlock()
	sortLines(out)
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

func sortLines(ls []Line) {
	slices.SortStableFunc(ls, func(a, b Line) int {
		if c := a.Time.Compare(b.Time); c != 0 {
			return c
		}
		switch {
		case a.Seq < b.Seq:
			return -1
		case a.Seq > b.Seq:
			return 1
		}
		return 0
	})
}

// Subscribe delivers every new line matching f until cancel is called.
func (m *Manager) Subscribe(f Filter, buffer int) (<-chan Line, func()) {
	s := &subscriber{f: f, ch: make(chan Line, buffer)}
	m.mu.Lock()
	m.subs[s] = struct{}{}
	m.mu.Unlock()
	var once sync.Once
	return s.ch, func() {
		once.Do(func() {
			m.mu.Lock()
			delete(m.subs, s)
			m.mu.Unlock()
		})
	}
}

// Previous returns the last tail lines of the previous instance of a
// restarted container.
func (m *Manager) Previous(ctx context.Context, src Source, tail int64) (string, error) {
	rc, err := m.open(ctx, src, &corev1.PodLogOptions{Container: src.Container, Previous: true, TailLines: &tail, Timestamps: true})
	if err != nil {
		return "", err
	}
	defer rc.Close()
	var sb strings.Builder
	if _, err := io.Copy(&sb, io.LimitReader(rc, 16*1024*1024)); err != nil {
		return sb.String(), err
	}
	return sb.String(), nil
}
