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

// Package logs follows container logs, keeps the recent lines of each in a
// ring buffer, parses substrate's JSON log envelope, and fans new lines out to
// subscribers.
package logs

import (
	"encoding/json"
	"strings"
	"time"
)

// Source identifies one container's log stream.
type Source struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container"`
}

func (s Source) String() string { return s.Namespace + "/" + s.Pod + "/" + s.Container }

// Line is one parsed log line.
type Line struct {
	// Seq orders lines globally in arrival order.
	Seq uint64 `json:"seq"`
	Source
	// Time is the application's own timestamp when the line carries one,
	// otherwise the kubelet's.
	Time  time.Time `json:"time"`
	Level string    `json:"level,omitempty"`
	Msg   string    `json:"msg,omitempty"`
	// Attrs holds the substrate attributes (keys starting with "ate.") of a
	// JSON line, stringified.
	Attrs map[string]string `json:"attrs,omitempty"`
	// Raw is the line as the container wrote it.
	Raw string `json:"raw"`
	// kubeTime is the kubelet timestamp, used to resume a stream.
	kubeTime time.Time
}

// parseLine parses one line of a log stream requested with Timestamps: true.
func parseLine(src Source, text string) Line {
	l := Line{Source: src, Raw: text}
	if ts, rest, ok := strings.Cut(text, " "); ok {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			l.kubeTime = t
			l.Time = t
			l.Raw = rest
		}
	}
	parseBody(&l)
	return l
}

// parseBody fills Level, Msg, Attrs and, when present, Time from a JSON body.
func parseBody(l *Line) {
	body := strings.TrimSpace(l.Raw)
	if !strings.HasPrefix(body, "{") {
		l.Level = guessLevel(body)
		return
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		l.Level = guessLevel(body)
		return
	}
	if v, ok := stringField(m, "time", "ts", "timestamp"); ok {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			l.Time = t
		}
	}
	if v, ok := stringField(m, "level", "severity", "lvl"); ok {
		l.Level = strings.ToUpper(v)
	}
	if v, ok := stringField(m, "msg", "message"); ok {
		l.Msg = v
	}
	for k, raw := range m {
		if !strings.HasPrefix(k, "ate.") {
			continue
		}
		if l.Attrs == nil {
			l.Attrs = map[string]string{}
		}
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			l.Attrs[k] = s
		} else {
			l.Attrs[k] = string(raw)
		}
	}
}

func stringField(m map[string]json.RawMessage, keys ...string) (string, bool) {
	for _, k := range keys {
		raw, ok := m[k]
		if !ok {
			continue
		}
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return s, true
		}
	}
	return "", false
}

// guessLevel recognizes the level of common non-JSON formats: klog's
// "E0102 ..." prefix and "level=error" logfmt.
func guessLevel(s string) string {
	if len(s) > 5 && s[1] >= '0' && s[1] <= '9' {
		switch s[0] {
		case 'E':
			return "ERROR"
		case 'W':
			return "WARN"
		case 'I':
			return "INFO"
		}
	}
	lower := strings.ToLower(s)
	for _, lvl := range []string{"error", "warn", "info", "debug"} {
		if strings.Contains(lower, "level="+lvl) {
			return strings.ToUpper(lvl)
		}
	}
	return ""
}

// LevelRank orders levels so a filter can ask for "WARN and above". Unknown
// levels rank as INFO.
func LevelRank(level string) int {
	switch strings.ToUpper(level) {
	case "DEBUG", "TRACE":
		return 0
	case "WARN", "WARNING":
		return 2
	case "ERROR", "FATAL", "PANIC", "CRITICAL":
		return 3
	default:
		return 1
	}
}
