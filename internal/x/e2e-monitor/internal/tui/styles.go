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
	"hash/fnv"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"

	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/discovery"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/e2efeed"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/monitor"
)

var (
	colorOK     = lipgloss.Color("2")
	colorWarn   = lipgloss.Color("3")
	colorErr    = lipgloss.Color("1")
	colorMuted  = lipgloss.Color("8")
	colorAccent = lipgloss.Color("12")

	styleTitle     = lipgloss.NewStyle().Bold(true)
	styleAccent    = lipgloss.NewStyle().Foreground(colorAccent)
	styleMuted     = lipgloss.NewStyle().Foreground(colorMuted)
	styleOK        = lipgloss.NewStyle().Foreground(colorOK)
	styleWarn      = lipgloss.NewStyle().Foreground(colorWarn)
	styleErr       = lipgloss.NewStyle().Foreground(colorErr)
	styleTab       = lipgloss.NewStyle().Padding(0, 1).Foreground(colorMuted)
	styleTabActive = lipgloss.NewStyle().Padding(0, 1).Bold(true).Foreground(lipgloss.Color("0")).Background(colorAccent)
	styleSelected  = lipgloss.NewStyle().Reverse(true)
	styleHeading   = lipgloss.NewStyle().Bold(true).Foreground(colorAccent)
	styleBox       = lipgloss.NewStyle().Border(lipgloss.NormalBorder(), true, false, false, false).BorderForeground(colorMuted)

	// lanePalette colors timeline lanes; LaneTest is always the accent.
	lanePalette = []lipgloss.Color{"5", "6", "11", "13", "14", "10", "9", "4", "3"}
)

func healthDot(h discovery.Health) string {
	switch h {
	case discovery.HealthOK:
		return styleOK.Render("●")
	case discovery.HealthDegraded:
		return styleWarn.Render("●")
	case discovery.HealthDown:
		return styleErr.Render("●")
	}
	return styleMuted.Render("○")
}

func stateIcon(state string) string {
	switch state {
	case e2efeed.StatePass:
		return styleOK.Render("✓")
	case e2efeed.StateFail, e2efeed.StateInterrupted:
		return styleErr.Render("✗")
	case e2efeed.StateRunning:
		return styleWarn.Render("●")
	case e2efeed.StatePaused:
		return styleWarn.Render("‖")
	case e2efeed.StateSkip:
		return styleMuted.Render("↷")
	}
	return styleMuted.Render("○")
}

func levelStyle(level string) lipgloss.Style {
	switch strings.ToUpper(level) {
	case "ERROR", "FATAL", "PANIC":
		return styleErr
	case "WARN", "WARNING":
		return styleWarn
	case "DEBUG", "TRACE":
		return styleMuted
	}
	return lipgloss.NewStyle()
}

func laneStyle(lane string) lipgloss.Style {
	if lane == monitor.LaneTest {
		return styleAccent
	}
	h := fnv.New32a()
	h.Write([]byte(lane))
	return lipgloss.NewStyle().Foreground(lanePalette[int(h.Sum32())%len(lanePalette)])
}

// fit truncates s to width cells, ANSI-aware, and pads it to exactly width.
func fit(s string, width int) string {
	if width <= 0 {
		return ""
	}
	s = ansi.Truncate(s, width, "…")
	if w := lipgloss.Width(s); w < width {
		s += strings.Repeat(" ", width-w)
	}
	return s
}

// clip truncates s to width cells without padding.
func clip(s string, width int) string {
	if width <= 0 {
		return ""
	}
	return ansi.Truncate(s, width, "…")
}
