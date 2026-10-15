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

// Package tui is the terminal frontend of e2e-monitor, built on Bubble Tea.
// It presents the same read model as the web UI: an overview, one tab per
// component with its pods and live logs, the worker pools, and the e2e run
// with each test's timeline through the components.
package tui

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/discovery"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/e2efeed"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/monitor"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/substrate"
)

// Source is the read model the TUI presents; *monitor.Monitor implements it.
type Source interface {
	Info() monitor.Info
	Components() []discovery.Component
	Pools() []discovery.Pool
	Substrate() substrate.State
	E2E() e2efeed.Summary
	Test(id string) (monitor.TestDetail, bool)
	Timeline(id string, opts monitor.TimelineOptions) (monitor.Timeline, bool)
	Subscribe(q monitor.LogQuery, buffer int) (*monitor.LogSubscription, error)
	Changes() (<-chan monitor.Topic, func())
}

var _ Source = (*monitor.Monitor)(nil)

// Run runs the TUI on the terminal until the user quits or ctx ends.
func Run(ctx context.Context, src Source) error {
	m := New(src)
	defer m.Close()
	_, err := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion(), tea.WithContext(ctx)).Run()
	if ctx.Err() != nil {
		return nil
	}
	return err
}

type tabKind int

const (
	tabOverview tabKind = iota
	tabComponent
	tabWorkers
	tabE2E
)

type tab struct {
	kind tabKind
	name string
}

func (t tab) key() string { return fmt.Sprintf("%d/%s", t.kind, t.name) }

func (t tab) title() string {
	switch t.kind {
	case tabOverview:
		return "Overview"
	case tabWorkers:
		return "Workers"
	case tabE2E:
		return "E2E"
	}
	return t.name
}

type (
	changeMsg monitor.Topic
	tickMsg   time.Time
	linesMsg  struct {
		gen   int
		lines []logs.Line
	}
)

const (
	maxLogLines      = 5000
	detailRefreshGap = 2 * time.Second
)

var levels = []string{"", "INFO", "WARN", "ERROR"}

// Model is the Bubble Tea model.
type Model struct {
	src           Source
	width, height int

	info  monitor.Info
	comps []discovery.Component
	pools []discovery.Pool
	sum   e2efeed.Summary
	subst substrate.State

	tabs   []tab
	active int
	cursor map[string]int

	// The log pane, shared by the component and worker tabs.
	logVP     viewport.Model
	logKey    string
	logTitle  string
	sub       *monitor.LogSubscription
	subDone   chan struct{}
	gen       int
	lines     []logs.Line
	follow    bool
	wrap      bool
	level     string
	container string
	// showEvents swaps a component's log pane for its events; showAPI swaps
	// the workers' log pane for ateapi's workers and actors.
	showEvents bool
	showAPI    bool

	// The E2E detail pane.
	detailVP     viewport.Model
	detailID     string
	detail       monitor.TestDetail
	timeline     monitor.Timeline
	detailOutput bool
	ownScope     bool
	refreshedAt  time.Time

	filter      textinput.Model
	filtering   bool
	help        bool
	changes     <-chan monitor.Topic
	stopChanges func()
}

// New builds a Model over src.
func New(src Source) *Model {
	fi := textinput.New()
	fi.Prompt = "/"
	fi.Placeholder = "filter text"
	m := &Model{
		src:      src,
		cursor:   map[string]int{},
		follow:   true,
		logVP:    viewport.New(0, 0),
		detailVP: viewport.New(0, 0),
		filter:   fi,
		info:     src.Info(),
	}
	m.changes, m.stopChanges = src.Changes()
	m.refresh("")
	return m
}

// Close releases the model's subscriptions.
func (m *Model) Close() {
	m.unsubscribe()
	if m.stopChanges != nil {
		m.stopChanges()
	}
}

func waitChange(ch <-chan monitor.Topic) tea.Cmd {
	return func() tea.Msg {
		topic, ok := <-ch
		if !ok {
			return nil
		}
		return changeMsg(topic)
	}
}

func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg { return tickMsg(t) })
}

// waitLines delivers the next batch of live log lines of subscription gen.
func waitLines(gen int, ch <-chan logs.Line, done <-chan struct{}) tea.Cmd {
	return func() tea.Msg {
		var batch []logs.Line
		select {
		case l := <-ch:
			batch = append(batch, l)
		case <-done:
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		for len(batch) < 500 {
			select {
			case l := <-ch:
				batch = append(batch, l)
			case <-timer.C:
				return linesMsg{gen: gen, lines: batch}
			case <-done:
				return nil
			}
		}
		return linesMsg{gen: gen, lines: batch}
	}
}

// Init starts listening for changes.
func (m *Model) Init() tea.Cmd {
	return tea.Batch(waitChange(m.changes), tick(), m.syncLogs())
}

// refresh reloads the data for topic ("" for all) and rebuilds the tabs.
func (m *Model) refresh(topic monitor.Topic) {
	if topic == "" || topic == monitor.TopicComponents {
		m.comps = m.src.Components()
		m.pools = m.src.Pools()
	}
	if topic == "" || topic == monitor.TopicE2E {
		m.sum = m.src.E2E()
	}
	if topic == "" || topic == monitor.TopicSubstrate {
		m.subst = m.src.Substrate()
	}
	var current string
	if m.active < len(m.tabs) {
		current = m.tabs[m.active].key()
	}
	m.tabs = []tab{{kind: tabOverview}}
	for _, c := range m.comps {
		m.tabs = append(m.tabs, tab{kind: tabComponent, name: c.Name})
	}
	m.tabs = append(m.tabs, tab{kind: tabWorkers}, tab{kind: tabE2E})
	m.active = 0
	for i, t := range m.tabs {
		if t.key() == current {
			m.active = i
		}
	}
}

func (m *Model) tab() tab { return m.tabs[m.active] }

func (m *Model) cur() int { return m.cursor[m.tab().key()] }

func (m *Model) setCur(i int) { m.cursor[m.tab().key()] = i }

// Update handles a message.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.layout()
		m.renderLogs()
		m.renderDetail()
		return m, nil

	case changeMsg:
		m.refresh(monitor.Topic(msg))
		cmds := []tea.Cmd{waitChange(m.changes), m.syncLogs()}
		if monitor.Topic(msg) == monitor.TopicE2E {
			m.syncDetail(false)
		}
		// Pod lists may have grown or shrunk, resizing the log pane.
		if monitor.Topic(msg) != monitor.TopicE2E {
			m.renderLogs()
		}
		return m, tea.Batch(cmds...)

	case tickMsg:
		// A running test's timeline grows; refresh it now and then.
		if m.tab().kind == tabE2E && m.detailID != "" && time.Since(m.refreshedAt) > detailRefreshGap {
			if t, ok := m.testSummary(m.detailID); ok && (t.State == e2efeed.StateRunning || t.State == e2efeed.StatePaused) {
				m.syncDetail(true)
			}
		}
		return m, tick()

	case linesMsg:
		if msg.gen != m.gen || m.sub == nil {
			return m, nil
		}
		for _, l := range msg.lines {
			if l.Seq > m.sub.LastSeq {
				m.lines = append(m.lines, l)
			}
		}
		if over := len(m.lines) - maxLogLines; over > 0 {
			m.lines = slices.Delete(m.lines, 0, over)
		}
		m.renderLogs()
		return m, waitLines(m.gen, m.sub.C, m.subDone)

	case tea.MouseMsg:
		var cmd tea.Cmd
		if m.tab().kind == tabE2E {
			m.detailVP, cmd = m.detailVP.Update(msg)
		} else {
			m.logVP, cmd = m.logVP.Update(msg)
			m.follow = m.logVP.AtBottom()
		}
		return m, cmd

	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.filtering {
		switch msg.String() {
		case "enter":
			m.filtering = false
			m.filter.Blur()
			return m, m.filterChanged()
		case "esc":
			m.filtering = false
			m.filter.Blur()
			m.filter.SetValue("")
			return m, m.filterChanged()
		}
		var cmd tea.Cmd
		m.filter, cmd = m.filter.Update(msg)
		return m, cmd
	}
	if m.help {
		m.help = false
		return m, nil
	}

	vp := &m.logVP
	if m.tab().kind == tabE2E {
		vp = &m.detailVP
	}
	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "?":
		m.help = true
	case "right", "l", "tab":
		return m, m.switchTab(m.active + 1)
	case "left", "h", "shift+tab":
		return m, m.switchTab(m.active - 1)
	case "1", "2", "3", "4", "5", "6", "7", "8", "9":
		return m, m.switchTab(int(msg.String()[0] - '1'))
	case "up", "k":
		return m, m.moveCursor(-1)
	case "down", "j":
		return m, m.moveCursor(1)
	case "enter":
		if m.tab().kind == tabOverview {
			return m, m.openOverviewRow()
		}
	case "pgup", "ctrl+u":
		vp.HalfPageUp()
		m.follow = false
	case "pgdown", "ctrl+d":
		vp.HalfPageDown()
		m.follow = vp.AtBottom()
	case "K", "shift+up":
		vp.ScrollUp(1)
		m.follow = false
	case "J", "shift+down":
		vp.ScrollDown(1)
		m.follow = vp.AtBottom()
	case "g", "home":
		vp.GotoTop()
		m.follow = false
	case "G", "end":
		vp.GotoBottom()
		m.follow = true
	case "f":
		m.follow = !m.follow
		if m.follow {
			vp.GotoBottom()
		}
	case "w":
		m.wrap = !m.wrap
		m.renderLogs()
		m.renderDetail()
	case "v":
		m.level = levels[(slices.Index(levels, m.level)+1)%len(levels)]
		return m, m.syncLogs()
	case "c":
		return m, m.cycleContainer()
	case "/":
		m.filtering = true
		return m, m.filter.Focus()
	case "esc":
		if m.filter.Value() != "" {
			m.filter.SetValue("")
			return m, m.filterChanged()
		}
	case "e":
		if m.tab().kind == tabComponent {
			m.showEvents = !m.showEvents
			m.renderLogs()
		}
	case "a":
		if m.tab().kind == tabWorkers {
			m.showAPI = !m.showAPI
			m.renderLogs()
		}
	case "t":
		if m.tab().kind == tabE2E {
			m.detailOutput = !m.detailOutput
			m.syncDetail(true)
		}
	case "o":
		if m.tab().kind == tabE2E {
			m.ownScope = !m.ownScope
			m.syncDetail(true)
		}
	case "r":
		m.refresh("")
		m.logKey = ""
		m.syncDetail(true)
		return m, m.syncLogs()
	}
	return m, nil
}

func (m *Model) filterChanged() tea.Cmd {
	m.renderDetail()
	return m.syncLogs()
}

func (m *Model) switchTab(i int) tea.Cmd {
	n := len(m.tabs)
	m.active = ((i % n) + n) % n
	m.container = ""
	m.layout()
	m.syncDetail(false)
	return m.syncLogs()
}

func (m *Model) moveCursor(d int) tea.Cmd {
	n := m.rowCount()
	if n == 0 {
		return nil
	}
	m.setCur(min(max(m.cur()+d, 0), n-1))
	m.container = ""
	m.syncDetail(false)
	return m.syncLogs()
}

func (m *Model) rowCount() int {
	switch m.tab().kind {
	case tabOverview:
		return len(m.comps) + 2
	case tabComponent:
		if c, ok := m.component(); ok {
			return len(c.Pods) + 1
		}
	case tabWorkers:
		return len(m.workerRows())
	case tabE2E:
		return len(m.sum.Tests)
	}
	return 0
}

func (m *Model) openOverviewRow() tea.Cmd {
	i := m.cur()
	switch {
	case i < len(m.comps):
		return m.switchTab(i + 1)
	case i == len(m.comps):
		return m.switchTab(len(m.tabs) - 2)
	default:
		return m.switchTab(len(m.tabs) - 1)
	}
}

func (m *Model) component() (discovery.Component, bool) {
	t := m.tab()
	if t.kind != tabComponent {
		return discovery.Component{}, false
	}
	for _, c := range m.comps {
		if c.Name == t.name {
			return c, true
		}
	}
	return discovery.Component{}, false
}

// workerRow is a pool header (pod == nil) or one of its pods.
type workerRow struct {
	pool discovery.Pool
	pod  *discovery.Pod
}

func (m *Model) workerRows() []workerRow {
	var rows []workerRow
	for _, p := range m.pools {
		if p.E2E && len(p.Pods) == 0 {
			continue
		}
		rows = append(rows, workerRow{pool: p})
		for i := range p.Pods {
			rows = append(rows, workerRow{pool: p, pod: &p.Pods[i]})
		}
	}
	return rows
}

// selectedPod returns the pod the cursor is on in the component or worker
// tab, if any.
func (m *Model) selectedPod() (discovery.Pod, bool) {
	switch m.tab().kind {
	case tabComponent:
		c, ok := m.component()
		if i := m.cur() - 1; ok && i >= 0 && i < len(c.Pods) {
			return c.Pods[i], true
		}
	case tabWorkers:
		rows := m.workerRows()
		if i := m.cur(); i < len(rows) && rows[i].pod != nil {
			return *rows[i].pod, true
		}
	}
	return discovery.Pod{}, false
}

func (m *Model) cycleContainer() tea.Cmd {
	p, ok := m.selectedPod()
	if !ok {
		return nil
	}
	names := []string{""}
	for _, c := range p.Containers {
		if c.LongRunning() {
			names = append(names, c.Name)
		}
	}
	m.container = names[(slices.Index(names, m.container)+1)%len(names)]
	return m.syncLogs()
}

// logQuery returns what the log pane should show for the current tab and
// selection, and a title for it; ok is false when it should show nothing.
func (m *Model) logQuery() (q monitor.LogQuery, title string, ok bool) {
	q = monitor.LogQuery{MinLevel: m.level, Contains: m.filter.Value(), Limit: 2000}
	switch m.tab().kind {
	case tabComponent:
		c, found := m.component()
		if !found {
			return q, "", false
		}
		if p, sel := m.selectedPod(); sel {
			q.Pod = p.Namespace + "/" + p.Name
			title = p.Name
		} else {
			q.Component = c.Name
			title = fmt.Sprintf("all %d pods", len(c.Pods))
		}
	case tabWorkers:
		p, sel := m.selectedPod()
		if !sel {
			return q, "", false
		}
		q.Pod = p.Namespace + "/" + p.Name
		title = q.Pod
	default:
		return q, "", false
	}
	q.Container = m.container
	if q.Container != "" {
		title += "/" + q.Container
	}
	return q, title, true
}

func (m *Model) unsubscribe() {
	if m.sub != nil {
		m.sub.Cancel()
		close(m.subDone)
		m.sub, m.subDone = nil, nil
	}
}

// syncLogs points the log pane at what the current view selects,
// resubscribing only when that changed.
func (m *Model) syncLogs() tea.Cmd {
	q, title, ok := m.logQuery()
	key := fmt.Sprintf("%v|%+v", ok, q)
	if key == m.logKey {
		return nil
	}
	m.logKey, m.logTitle = key, title
	m.unsubscribe()
	m.gen++
	m.lines = nil
	if !ok {
		m.renderLogs()
		return nil
	}
	sub, err := m.src.Subscribe(q, 1024)
	if err != nil {
		m.logTitle = title + " — " + err.Error()
		m.renderLogs()
		return nil
	}
	m.sub, m.subDone = sub, make(chan struct{})
	m.lines = slices.Clone(sub.Backlog)
	m.follow = true
	m.renderLogs()
	return waitLines(m.gen, sub.C, m.subDone)
}

func (m *Model) testSummary(id string) (e2efeed.Test, bool) {
	for _, t := range m.sum.Tests {
		if t.ID() == id {
			return t, true
		}
	}
	return e2efeed.Test{}, false
}

// syncDetail loads the selected test's detail when the selection changed, or
// always when force is set.
func (m *Model) syncDetail(force bool) {
	if m.tab().kind != tabE2E {
		return
	}
	id := ""
	if i := m.cur(); i < len(m.sum.Tests) {
		id = m.sum.Tests[i].ID()
	}
	if id == m.detailID && !force {
		return
	}
	changed := id != m.detailID
	m.detailID = id
	m.refreshedAt = time.Now()
	if id == "" {
		m.renderDetail()
		return
	}
	m.detail, _ = m.src.Test(id)
	if !m.detailOutput {
		m.timeline, _ = m.src.Timeline(id, monitor.TimelineOptions{Own: m.ownScope})
	}
	atBottom := m.detailVP.AtBottom()
	m.renderDetail()
	if changed {
		m.detailVP.GotoTop()
	} else if atBottom {
		m.detailVP.GotoBottom()
	}
}

// layout sizes the panes for the current tab.
func (m *Model) layout() {
	if m.width == 0 || m.tabs == nil {
		return
	}
	body := m.bodyHeight()
	switch m.tab().kind {
	case tabComponent, tabWorkers:
		listH := m.listHeight()
		m.logVP.Width = m.width
		m.logVP.Height = max(body-listH-3, 1)
	case tabE2E:
		m.detailVP.Width = m.width - m.leftWidth() - 1
		m.detailVP.Height = max(body-4, 1)
	}
}

func (m *Model) bodyHeight() int { return max(m.height-3, 1) }

// listHeight is the height of the pods list above the log pane.
func (m *Model) listHeight() int {
	return min(m.rowCount(), max(m.bodyHeight()/3, 3))
}

func (m *Model) leftWidth() int { return min(max(m.width*2/5, 30), 60) }
