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

package stress

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

// Steady-state benchmarks of one query round trip through the relay to a
// fast upstream. Run with:
//
//	go test ./internal/ateomnet/dns/stress -run '^$' -bench . -benchtime=5s

func BenchmarkUDPExchange(b *testing.B) {
	sb := newHarness(b).addSandbox()
	conn := dialBench(b, "udp", sb.endpoint.udp)
	var h histogram
	buf := make([]byte, 65535)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		udpRoundTrip(b, conn, uint16(i), buf, &h)
	}
	reportLatency(b, &h)
}

// BenchmarkUDPExchangeParallel keeps one query outstanding per goroutine,
// capped at 32 so a closed loop stays within the relay's 64 slots (the relay
// frees a slot only after writing the answer).
func BenchmarkUDPExchangeParallel(b *testing.B) {
	sb := newHarness(b).addSandbox()
	var h histogram
	slots := make(chan struct{}, 32)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			return
		}
		conn := dialBench(b, "udp", sb.endpoint.udp)
		buf := make([]byte, 65535)
		for i := 0; pb.Next(); i++ {
			udpRoundTrip(b, conn, uint16(i), buf, &h)
		}
	})
	reportLatency(b, &h)
}

func BenchmarkTCPExchange(b *testing.B) {
	sb := newHarness(b).addSandbox()
	conn := dialBench(b, "tcp", sb.endpoint.tcp)
	var h histogram
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		q := buildQuery(uint16(i), "example.com", typeA)
		start := time.Now()
		_ = conn.SetDeadline(start.Add(5 * time.Second))
		if _, err := conn.Write(append(binary.BigEndian.AppendUint16(nil, uint16(len(q))), q...)); err != nil {
			b.Fatal(err)
		}
		var prefix [2]byte
		if _, err := io.ReadFull(conn, prefix[:]); err != nil {
			b.Fatal(err)
		}
		resp := make([]byte, binary.BigEndian.Uint16(prefix[:]))
		if _, err := io.ReadFull(conn, resp); err != nil {
			b.Fatal(err)
		}
		if _, err := checkAnswer(q, resp); err != nil {
			b.Fatal(err)
		}
		h.record(time.Since(start))
	}
	reportLatency(b, &h)
}

func dialBench(b *testing.B, network, addr string) net.Conn {
	conn, err := net.Dial(network, addr)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = conn.Close() })
	return conn
}

func udpRoundTrip(b *testing.B, conn net.Conn, id uint16, buf []byte, h *histogram) {
	q := buildQuery(id, "example.com", typeA)
	start := time.Now()
	_ = conn.SetDeadline(start.Add(5 * time.Second))
	if _, err := conn.Write(q); err != nil {
		b.Error(err)
		return
	}
	for {
		n, err := conn.Read(buf)
		if err != nil {
			b.Error(err)
			return
		}
		if messageID(buf[:n]) != id {
			continue
		}
		if _, err := checkAnswer(q, buf[:n]); err != nil {
			b.Error(err)
			return
		}
		h.record(time.Since(start))
		return
	}
}

func reportLatency(b *testing.B, h *histogram) {
	b.ReportMetric(float64(h.quantile(0.5).Microseconds()), "p50-µs")
	b.ReportMetric(float64(h.quantile(0.99).Microseconds()), "p99-µs")
}
