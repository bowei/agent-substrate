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

package tui

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/discovery"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/e2efeed"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
)

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "            "
	}
	return t.Local().Format("15:04:05.000")
}

func age(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func secs(s float64) string {
	if s == 0 {
		return ""
	}
	return fmt.Sprintf("%.1fs", s)
}

// window returns the [start, end) rows of n to show in height rows so that
// cursor stays visible.
func window(n, cursor, height int) (int, int) {
	if n <= height {
		return 0, n
	}
	start := min(max(cursor-height/2, 0), n-height)
	return start, start + height
}

// View renders the screen.
func (m *Model) View() string {
	if m.width == 0 {
		return "loading…"
	}
	var body string
	if m.help {
		body = helpText()
	} else {
		switch m.tab().kind {
		case tabOverview:
			body = m.viewOverview()
		case tabComponent:
			body = m.viewComponent()
		case tabWorkers:
			body = m.viewWorkers()
		case tabE2E:
			body = m.viewE2E()
		}
	}
	body = lipgloss.NewStyle().Height(m.bodyHeight()).MaxHeight(m.bodyHeight()).MaxWidth(m.width).Render(body)
	return lipgloss.JoinVertical(lipgloss.Left, m.viewHeader(), m.viewTabs(), body, m.viewFooter())
}

func (m *Model) viewHeader() string {
	s := m.sum
	run := styleMuted.Render("no e2e run")
	if s.Counts.Total > 0 || s.Active {
		state := styleOK.Render("finished")
		if s.Active {
			state = styleWarn.Render("running")
		}
		run = fmt.Sprintf("e2e %s %s %s %s  %s",
			styleOK.Render(fmt.Sprintf("✓%d", s.Counts.Pass)),
			styleErr.Render(fmt.Sprintf("✗%d", s.Counts.Fail)),
			styleWarn.Render(fmt.Sprintf("●%d", s.Counts.Running)),
			styleMuted.Render(fmt.Sprintf("↷%d", s.Counts.Skip)), state)
	}
	ctx := m.info.Context
	if ctx == "" {
		ctx = "(current)"
	}
	line := fmt.Sprintf("%s  %s  %s  %s",
		styleTitle.Render("Substrate e2e-monitor"),
		styleMuted.Render("context "+ctx),
		styleMuted.Render("ns "+m.info.SystemNamespace),
		run)
	return clip(line, m.width)
}

func (m *Model) viewTabs() string {
	rendered := make([]string, len(m.tabs))
	for i, t := range m.tabs {
		title := t.title()
		switch t.kind {
		case tabComponent:
			for _, c := range m.comps {
				if c.Name == t.name {
					title = healthDot(c.Health) + " " + title
				}
			}
		case tabE2E:
			if m.sum.Counts.Fail > 0 {
				title = styleErr.Render("✗") + " " + title
			} else if m.sum.Active {
				title = styleWarn.Render("●") + " " + title
			}
		}
		if i == m.active {
			rendered[i] = styleTabActive.Render(t.title())
		} else {
			rendered[i] = styleTab.Render(title)
		}
	}
	// Scroll the bar so the active tab stays visible.
	start := 0
	for start < m.active && lipgloss.Width(strings.Join(rendered[start:m.active+1], "")) > m.width {
		start++
	}
	return clip(strings.Join(rendered[start:], ""), m.width)
}

func (m *Model) viewFooter() string {
	if m.filtering {
		return clip(m.filter.View(), m.width)
	}
	var keys string
	switch m.tab().kind {
	case tabOverview:
		keys = "↑↓ select · enter open"
	case tabComponent:
		keys = "↑↓ pod · c container · v level · / filter · f follow · w wrap · e events"
	case tabWorkers:
		keys = "↑↓ pod · c container · v level · / filter · f follow · a ateapi"
	case tabE2E:
		keys = "↑↓ test · t timeline/output · o own resources · / filter · pgup/pgdn scroll"
	}
	keys += " · ←→ tabs · ? help · q quit"
	if f := m.filter.Value(); f != "" {
		keys = styleAccent.Render("filter: "+f) + "  " + keys
	}
	return styleMuted.Render(clip(keys, m.width))
}

func helpText() string {
	rows := [][2]string{
		{"←/→, h/l, tab", "previous/next tab"},
		{"1-9", "jump to tab"},
		{"↑/↓, k/j", "move the selection (pod, test)"},
		{"pgup/pgdn, ctrl+u/d", "scroll the log or detail pane"},
		{"J/K", "scroll one line"},
		{"g/G", "top/bottom of the pane"},
		{"f", "follow new lines"},
		{"w", "wrap long lines"},
		{"v", "cycle the minimum log level"},
		{"c", "cycle the container of the selected pod"},
		{"/", "filter text (enter applies, esc clears)"},
		{"e", "component: show events instead of logs"},
		{"a", "workers: show ateapi workers and actors instead of logs"},
		{"t", "e2e: switch between timeline and test output"},
		{"o", "e2e: correlate only the test's own resources"},
		{"r", "reload everything"},
		{"q, ctrl+c", "quit"},
	}
	var b strings.Builder
	b.WriteString(styleHeading.Render("Keys") + "\n\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "  %s %s\n", styleAccent.Render(fmt.Sprintf("%-22s", r[0])), r[1])
	}
	b.WriteString("\n" + styleMuted.Render("Press any key to close."))
	return b.String()
}

// ---------------------------------------------------------------------------
// Overview.

func (m *Model) viewOverview() string {
	var rows []string
	header := styleMuted.Render(fmt.Sprintf("   %-34s %-12s %-8s %-6s %-9s %s", "COMPONENT", "KIND", "READY", "PODS", "RESTARTS", "NAMESPACE"))
	for _, c := range m.comps {
		restarts := int32(0)
		for _, p := range c.Pods {
			restarts += p.Restarts
		}
		warn := ""
		for _, e := range c.Events {
			if e.Type == "Warning" {
				warn = styleWarn.Render(" warnings")
				break
			}
		}
		rows = append(rows, fmt.Sprintf("%s %-34s %-12s %-8s %-6d %-9d %s%s", healthDot(c.Health), clip(c.Name, 34), c.Kind,
			fmt.Sprintf("%d/%d", c.Ready, c.Desired), len(c.Pods), restarts, c.Namespace, warn))
	}
	var pods, e2ePools int
	degraded := false
	for _, p := range m.pools {
		pods += len(p.Pods)
		if p.E2E {
			e2ePools++
		}
		if p.Ready < p.Replicas {
			degraded = true
		}
	}
	h := discovery.HealthOK
	if degraded {
		h = discovery.HealthDegraded
	}
	api := styleMuted.Render("ateapi unavailable")
	if m.subst.APIError == "" && !m.subst.Updated.IsZero() {
		api = fmt.Sprintf("%d registered, %d actors", len(m.subst.Workers), len(m.subst.Actors))
	}
	rows = append(rows, fmt.Sprintf("%s %-34s %d pools (%d e2e), %d pods · %s", healthDot(h), "Workers", len(m.pools), e2ePools, pods, api))
	e2e := m.sum.Source
	if e2e == "" {
		e2e = styleMuted.Render("no run attached")
	}
	rows = append(rows, fmt.Sprintf("%s %-34s %s", stateIcon(m.runState()), "E2E run", e2e))

	var b strings.Builder
	b.WriteString(clip(header, m.width) + "\n")
	for i, r := range rows {
		line := fit(r, m.width)
		if i == m.cur() {
			line = styleSelected.Render(line)
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}

func (m *Model) runState() string {
	switch {
	case m.sum.Counts.Total == 0 && !m.sum.Active:
		return ""
	case m.sum.Counts.Fail > 0:
		return e2efeed.StateFail
	case m.sum.Active:
		return e2efeed.StateRunning
	}
	return e2efeed.StatePass
}

// ---------------------------------------------------------------------------
// Component and worker tabs.

func podLine(p discovery.Pod) string {
	status := p.Phase
	style := styleOK
	if p.Reason != "" {
		status = p.Reason
		style = styleErr
		if p.Reason == "Terminating" {
			style = styleWarn
		}
	} else if p.Phase != "Running" {
		style = styleWarn
	}
	restarts := fmt.Sprintf("%d", p.Restarts)
	if p.Restarts > 0 {
		restarts = styleWarn.Render(restarts)
	}
	return fmt.Sprintf("%-48s %-5s %s %8s  %-5s %s", clip(p.Name, 48), p.Ready, fit(style.Render(status), 18), restarts, age(p.Created), styleMuted.Render(p.Node))
}

func (m *Model) renderList(rows []string) string {
	h := m.listHeight()
	start, end := window(len(rows), m.cur(), h)
	var b strings.Builder
	for i := start; i < end; i++ {
		line := fit(rows[i], m.width)
		if i == m.cur() {
			line = styleSelected.Render(line)
		}
		b.WriteString(line + "\n")
	}
	for i := end - start; i < h; i++ {
		b.WriteString("\n")
	}
	return b.String()
}

func (m *Model) paneTitle(kind string) string {
	parts := []string{styleHeading.Render(kind), m.logTitle}
	if m.level != "" {
		parts = append(parts, "level ≥ "+m.level)
	}
	if f := m.filter.Value(); f != "" {
		parts = append(parts, fmt.Sprintf("filter %q", f))
	}
	if m.follow {
		parts = append(parts, styleOK.Render("following"))
	} else {
		parts = append(parts, styleWarn.Render("paused (G to follow)"))
	}
	parts = append(parts, styleMuted.Render(fmt.Sprintf("%d lines", len(m.lines))))
	return styleBox.Width(m.width).Render(clip(strings.Join(parts, " · "), m.width))
}

func (m *Model) viewComponent() string {
	c, ok := m.component()
	if !ok {
		return styleErr.Render("This component no longer exists.")
	}
	info := fmt.Sprintf("%s %s  %s in %s · %d/%d ready", healthDot(c.Health), styleTitle.Render(c.Name), c.Kind, c.Namespace, c.Ready, c.Desired)
	if c.Description != "" {
		info += styleMuted.Render(" · " + c.Description)
	}
	rows := []string{styleAccent.Render(fmt.Sprintf("all pods (%d)", len(c.Pods)))}
	for _, p := range c.Pods {
		rows = append(rows, podLine(p))
	}
	title := m.paneTitle("logs")
	if m.showEvents {
		title = styleBox.Width(m.width).Render(styleHeading.Render("events") + styleMuted.Render(fmt.Sprintf(" · %d recent (e for logs)", len(c.Events))))
	}
	return clip(info, m.width) + "\n" + m.renderList(rows) + title + "\n" + m.logVP.View()
}

func (m *Model) viewWorkers() string {
	rows := m.workerRows()
	lines := make([]string, len(rows))
	for i, r := range rows {
		if r.pod == nil {
			p := r.pool
			e2e := ""
			if p.E2E {
				e2e = styleAccent.Render(" e2e")
				if p.Test != "" {
					e2e += " " + p.Test
				}
			}
			h := discovery.HealthOK
			if p.Ready < p.Replicas {
				h = discovery.HealthDegraded
			}
			lines[i] = fmt.Sprintf("%s %s %s %s%s", healthDot(h), styleTitle.Render(p.Namespace+"/"+p.Name),
				fmt.Sprintf("%d/%d ready", p.Ready, p.Replicas), styleMuted.Render(p.SandboxClass), e2e)
			continue
		}
		lines[i] = "    " + podLine(*r.pod)
	}
	if len(lines) == 0 {
		lines = []string{styleMuted.Render("No worker pools found.")}
	}
	title := m.paneTitle("logs")
	if m.showAPI {
		title = styleBox.Width(m.width).Render(styleHeading.Render("ateapi") + styleMuted.Render(" · registered workers and actors (a for logs)"))
	} else if _, ok := m.selectedPod(); !ok {
		title = styleBox.Width(m.width).Render(styleMuted.Render("Select a worker pod to stream its logs (followed on demand) · a for ateapi state"))
	}
	return m.renderList(lines) + title + "\n" + m.logVP.View()
}

// renderLogs fills the log pane for the component and worker tabs.
func (m *Model) renderLogs() {
	m.layout()
	if m.width == 0 {
		return
	}
	var content string
	switch {
	case m.tab().kind == tabComponent && m.showEvents:
		content = m.eventsContent()
	case m.tab().kind == tabWorkers && m.showAPI:
		content = m.apiContent()
	default:
		var b strings.Builder
		for i, l := range m.lines {
			if i > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(m.logLine(l))
		}
		content = b.String()
		if len(m.lines) == 0 && m.sub != nil {
			content = styleMuted.Render("No lines yet; new lines appear here as they arrive.")
		}
	}
	m.logVP.SetContent(content)
	if m.follow {
		m.logVP.GotoBottom()
	}
}

func (m *Model) logLine(l logs.Line) string {
	src := l.Container
	if m.tab().kind == tabComponent {
		if _, sel := m.selectedPod(); !sel {
			src = l.Pod + "/" + l.Container
		}
	}
	msg := l.Raw
	if l.Msg != "" {
		msg = l.Msg
		if kv := extraFields(l.Raw); kv != "" {
			msg += styleMuted.Render("  " + kv)
		}
	}
	lvl := levelStyle(l.Level).Render(fmt.Sprintf("%-5s", clip(l.Level, 5)))
	line := fmt.Sprintf("%s %s %s %s", styleMuted.Render(fmtTime(l.Time)), lipgloss.NewStyle().Foreground(lipgloss.Color("4")).Render(src), lvl, levelStyle(l.Level).Render(msg))
	if m.wrap {
		return lipgloss.NewStyle().Width(m.width).Render(line)
	}
	return clip(line, m.width)
}

// extraFields renders the fields of a JSON log line other than time, level,
// and message as k=v pairs.
func extraFields(raw string) string {
	var fields map[string]any
	if json.Unmarshal([]byte(raw), &fields) != nil {
		return ""
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		switch k {
		case "time", "level", "msg", "message":
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v := fields[k]
		if s, ok := v.(string); ok {
			parts = append(parts, k+"="+s)
			continue
		}
		b, _ := json.Marshal(v)
		parts = append(parts, k+"="+string(b))
	}
	s := strings.Join(parts, " ")
	if len(s) > 600 {
		s = s[:600] + "…"
	}
	return s
}

func (m *Model) eventsContent() string {
	c, ok := m.component()
	if !ok || len(c.Events) == 0 {
		return styleMuted.Render("No recent events.")
	}
	var b strings.Builder
	for _, e := range c.Events {
		typ := e.Type
		if typ == "Warning" {
			typ = styleWarn.Render(typ)
		}
		count := ""
		if e.Count > 1 {
			count = styleMuted.Render(fmt.Sprintf(" (×%d)", e.Count))
		}
		fmt.Fprintln(&b, clip(fmt.Sprintf("%5s ago  %-8s %-20s %-40s %s%s", age(e.Time), typ, e.Reason, e.Object, e.Message, count), m.width))
	}
	return b.String()
}

func (m *Model) apiContent() string {
	s := m.subst
	var b strings.Builder
	if s.APIError != "" {
		b.WriteString(styleErr.Render(s.APIError) + "\n")
	}
	fmt.Fprintf(&b, "%s\n", styleHeading.Render(fmt.Sprintf("Registered workers (%d)", len(s.Workers))))
	for _, w := range s.Workers {
		state := styleOK.Render(w.State)
		if w.State != "active" {
			state = styleWarn.Render(w.State)
		}
		fmt.Fprintln(&b, clip(fmt.Sprintf("  %-56s %-16s %s  %s", w.Namespace+"/"+w.Pod, w.Pool, state, strings.Join(w.Actors, ", ")), m.width))
	}
	fmt.Fprintf(&b, "\n%s\n", styleHeading.Render(fmt.Sprintf("Actors (%d)", len(s.Actors))))
	f := strings.ToLower(m.filter.Value())
	for _, a := range s.Actors {
		line := fmt.Sprintf("  %-56s %-10s %s", a.Atespace+"/"+a.Name, a.State, a.WorkerPod)
		if f != "" && !strings.Contains(strings.ToLower(line), f) {
			continue
		}
		if a.Crash != "" {
			line += styleErr.Render("  " + a.Crash)
		}
		fmt.Fprintln(&b, clip(line, m.width))
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// E2E tab.

func (m *Model) viewE2E() string {
	lw := m.leftWidth()
	left := m.viewTestTree(lw)
	right := m.viewDetail(m.width - lw - 1)
	sep := styleMuted.Render(strings.Repeat("│\n", m.bodyHeight()))
	return lipgloss.JoinHorizontal(lipgloss.Top, left, sep, right)
}

func shortPkg(p string) string {
	if p == "" {
		return "(unattributed output)"
	}
	parts := strings.Split(p, "/")
	if len(parts) > 2 {
		parts = parts[len(parts)-2:]
	}
	return strings.Join(parts, "/")
}

func (m *Model) viewTestTree(width int) string {
	h := m.bodyHeight()
	box := lipgloss.NewStyle().Width(width).Height(h).MaxHeight(h)
	if len(m.sum.Tests) == 0 {
		return box.Render(styleMuted.Render(strings.Join([]string{
			"No e2e run attached.",
			"",
			"Run tests through the monitor:",
			"  e2e-monitor run --tui -- \\",
			"    hack/run-e2e.sh -run TestX",
			"",
			"or follow a test2json file:",
			"  e2e-monitor serve --tui \\",
			"    --e2e-json=/tmp/e2e.json",
		}, "\n")))
	}
	var lines []string
	selLine := 0
	pkg := "\x00"
	for i, t := range m.sum.Tests {
		if t.Package != pkg {
			pkg = t.Package
			lines = append(lines, styleMuted.Render(clip(shortPkg(pkg), width)))
		}
		depth := strings.Count(t.Name, "/")
		name := t.Name
		if depth > 0 {
			name = name[strings.LastIndex(name, "/")+1:]
		}
		dur := secs(t.Elapsed)
		if t.State == e2efeed.StateRunning || t.State == e2efeed.StatePaused {
			dur = age(t.Start)
		}
		text := fmt.Sprintf("%s%s %s", strings.Repeat("  ", depth+1), stateIcon(t.State), name)
		durW := lipgloss.Width(dur)
		line := fit(text, width-durW-1) + " " + styleMuted.Render(dur)
		if i == m.cur() {
			line = styleSelected.Render(fit(text, width-durW-1) + " " + dur)
			selLine = len(lines)
		}
		lines = append(lines, line)
	}
	start, end := window(len(lines), selLine, h)
	return box.Render(strings.Join(lines[start:end], "\n"))
}

func (m *Model) viewDetail(width int) string {
	if m.detailID == "" {
		return styleMuted.Render("Select a test.")
	}
	t := m.detail
	var refs []string
	for _, r := range t.AllRefs {
		name := r.Name
		if r.Atespace != "" {
			name = r.Atespace + "/" + name
		}
		refs = append(refs, string(r.Kind)+":"+name)
	}
	refLine := styleMuted.Render("no tracked resources: component logs cannot be correlated")
	if len(refs) > 0 {
		refLine = styleAccent.Render(strings.Join(refs, "  "))
	}
	mode := "timeline"
	if m.detailOutput {
		mode = "output"
	} else {
		if m.ownScope {
			mode += " (own resources)"
		}
		mode += styleMuted.Render(" · lanes: ")
		for i, l := range m.timeline.Lanes {
			if i > 0 {
				mode += " "
			}
			mode += laneStyle(l).Render(l)
		}
	}
	head := []string{
		clip(fmt.Sprintf("%s %s  %s %s", stateIcon(t.State), styleTitle.Render(t.Name), styleMuted.Render(shortPkg(t.Package)), secs(t.Elapsed)), width),
		clip(refLine, width),
		clip(styleHeading.Render(mode), width),
		styleMuted.Render(strings.Repeat("─", width)),
	}
	return strings.Join(head, "\n") + "\n" + m.detailVP.View()
}

// renderDetail fills the E2E detail pane.
func (m *Model) renderDetail() {
	m.layout()
	width := m.detailVP.Width
	if width <= 0 || m.detailID == "" {
		m.detailVP.SetContent("")
		return
	}
	f := strings.ToLower(m.filter.Value())
	var lines []string
	if m.detailOutput {
		for _, o := range m.detail.Output {
			if f != "" && !strings.Contains(strings.ToLower(o.Text), f) {
				continue
			}
			lines = append(lines, m.detailLine(styleMuted.Render(fmtTime(o.Time))+" "+o.Text, width))
		}
	} else {
		laneW := 0
		for _, l := range m.timeline.Lanes {
			laneW = max(laneW, len(l))
		}
		laneW = min(laneW, 22)
		for _, e := range m.timeline.Entries {
			if f != "" && !strings.Contains(strings.ToLower(e.Text+" "+e.Lane), f) {
				continue
			}
			text := e.Text
			if e.Kind == "log" {
				text = levelStyle(e.Level).Render(text)
			}
			lines = append(lines, m.detailLine(fmt.Sprintf("%s %s │ %s", styleMuted.Render(fmtTime(e.Time)), laneStyle(e.Lane).Render(fit(e.Lane, laneW)), text), width))
		}
		if m.timeline.Truncated {
			lines = append([]string{styleWarn.Render("log lines were capped; the earliest are not shown")}, lines...)
		}
	}
	if len(lines) == 0 {
		lines = []string{styleMuted.Render("Nothing to show yet.")}
	}
	m.detailVP.SetContent(strings.Join(lines, "\n"))
}

func (m *Model) detailLine(s string, width int) string {
	if m.wrap {
		return lipgloss.NewStyle().Width(width).Render(s)
	}
	return clip(s, width)
}
