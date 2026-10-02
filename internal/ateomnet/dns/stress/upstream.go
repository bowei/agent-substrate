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
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// behavior is how an [upstream] answers. The zero value answers every query
// at once with NOERROR and one A record.
type behavior struct {
	// delay is added before every answer; jitter adds up to that much more,
	// uniformly distributed.
	delay  time.Duration
	jitter time.Duration

	// rcode is the response code of every answer.
	rcode int

	// dropFraction of queries, in [0, 1], are never answered. 1 blackholes.
	dropFraction float64

	// garbage answers with random bytes instead of a DNS message.
	garbage bool

	// answerSize pads successful answers with A records up to this many bytes.
	answerSize int

	// tcpCloseOnAccept closes every TCP connection as soon as it is accepted.
	tcpCloseOnAccept bool
}

// upstreamStats counts what an [upstream] has seen.
type upstreamStats struct {
	Queries      int64 // queries received, UDP and TCP
	Answered     int64 // answers sent
	Dropped      int64 // queries deliberately left unanswered
	TCPConns     int64 // TCP connections accepted
	InFlight     int64 // queries waiting out their delay now
	PeakInFlight int64 // most queries waiting out their delay at once
}

// upstream is a fake resolver on 127.0.0.1 serving UDP and TCP on one port.
// Its behavior can be changed while it runs.
type upstream struct {
	packet   net.PacketConn
	stream   net.Listener
	behavior atomic.Pointer[behavior]

	queries, answered, dropped, tcpConns atomic.Int64
	inFlight, peakInFlight               atomic.Int64

	closed chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
}

// startUpstream starts a fake resolver that behaves as b.
func startUpstream(b behavior) (*upstream, error) {
	// Bind TCP first: its port is less likely to be taken for UDP than the
	// reverse, so this rarely retries.
	for range 10 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		pc, err := net.ListenPacket("udp", l.Addr().String())
		if err != nil {
			_ = l.Close()
			continue
		}
		u := &upstream{packet: pc, stream: l, closed: make(chan struct{}), conns: map[net.Conn]struct{}{}}
		u.setBehavior(b)
		u.wg.Add(2)
		go u.servePacket()
		go u.serveTCP()
		return u, nil
	}
	return nil, errors.New("stress: could not bind UDP and TCP on one loopback port")
}

// addr is the "host:port" the upstream serves on.
func (u *upstream) addr() string { return u.stream.Addr().String() }

// setBehavior changes how queries arriving from now on are answered.
func (u *upstream) setBehavior(b behavior) { u.behavior.Store(&b) }

// stats returns the upstream's counters.
func (u *upstream) stats() upstreamStats {
	return upstreamStats{
		Queries:      u.queries.Load(),
		Answered:     u.answered.Load(),
		Dropped:      u.dropped.Load(),
		TCPConns:     u.tcpConns.Load(),
		InFlight:     u.inFlight.Load(),
		PeakInFlight: u.peakInFlight.Load(),
	}
}

// resetPeak restarts PeakInFlight from the current in-flight count.
func (u *upstream) resetPeak() { u.peakInFlight.Store(u.inFlight.Load()) }

// close stops serving and waits for every answer still pending.
func (u *upstream) close() error {
	close(u.closed)
	err := errors.Join(u.packet.Close(), u.stream.Close())
	u.mu.Lock()
	for c := range u.conns {
		_ = c.Close()
	}
	u.mu.Unlock()
	u.wg.Wait()
	return err
}

// respond decides the answer to query, waiting out the delay. It returns nil
// when the query is to go unanswered.
func (u *upstream) respond(query []byte) []byte {
	u.queries.Add(1)
	b := u.behavior.Load()
	if b.dropFraction > 0 && rand.Float64() < b.dropFraction {
		u.dropped.Add(1)
		return nil
	}
	if delay := b.delay + jitter(b.jitter); delay > 0 {
		n := u.inFlight.Add(1)
		for {
			peak := u.peakInFlight.Load()
			if n <= peak || u.peakInFlight.CompareAndSwap(peak, n) {
				break
			}
		}
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-u.closed:
			t.Stop()
		}
		u.inFlight.Add(-1)
	}
	if b.garbage {
		g := make([]byte, 4+rand.IntN(60))
		for i := range g {
			g[i] = byte(rand.Uint32())
		}
		return g
	}
	resp, err := buildAnswer(query, b.rcode, b.answerSize)
	if err != nil {
		u.dropped.Add(1)
		return nil
	}
	return resp
}

func jitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return rand.N(max)
}

func (u *upstream) servePacket() {
	defer u.wg.Done()
	buf := make([]byte, 65535)
	for {
		n, from, err := u.packet.ReadFrom(buf)
		if err != nil {
			return
		}
		query := make([]byte, n)
		copy(query, buf[:n])
		u.wg.Add(1)
		go func() {
			defer u.wg.Done()
			if resp := u.respond(query); resp != nil {
				if _, err := u.packet.WriteTo(resp, from); err == nil {
					u.answered.Add(1)
				}
			}
		}()
	}
}

func (u *upstream) serveTCP() {
	defer u.wg.Done()
	for {
		conn, err := u.stream.Accept()
		if err != nil {
			return
		}
		u.tcpConns.Add(1)
		if u.behavior.Load().tcpCloseOnAccept {
			_ = conn.Close()
			continue
		}
		u.mu.Lock()
		u.conns[conn] = struct{}{}
		u.mu.Unlock()
		u.wg.Add(1)
		go func() {
			defer u.wg.Done()
			u.serveConn(conn)
			u.mu.Lock()
			delete(u.conns, conn)
			u.mu.Unlock()
		}()
	}
}

// serveConn answers each length-prefixed query on its own goroutine, so a
// delayed query does not hold up the ones pipelined behind it.
func (u *upstream) serveConn(conn net.Conn) {
	defer conn.Close()
	var writeMu sync.Mutex
	var pending sync.WaitGroup
	defer pending.Wait()
	for {
		var prefix [2]byte
		if _, err := io.ReadFull(conn, prefix[:]); err != nil {
			return
		}
		query := make([]byte, binary.BigEndian.Uint16(prefix[:]))
		if _, err := io.ReadFull(conn, query); err != nil {
			return
		}
		pending.Add(1)
		go func() {
			defer pending.Done()
			resp := u.respond(query)
			if resp == nil {
				return
			}
			writeMu.Lock()
			defer writeMu.Unlock()
			if _, err := conn.Write(binary.BigEndian.AppendUint16(nil, uint16(len(resp)))); err != nil {
				return
			}
			if _, err := conn.Write(resp); err == nil {
				u.answered.Add(1)
			}
		}()
	}
}
