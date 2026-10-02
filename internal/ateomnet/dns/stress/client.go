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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// proto is the transport a [load] queries over.
type proto int

const (
	protoUDP proto = iota
	protoTCP
)

func (p proto) String() string {
	if p == protoTCP {
		return "tcp"
	}
	return "udp"
}

// load is the traffic one simulated actor sends to its sandbox's endpoint.
//
// With QPS set the load is open loop: queries are sent on schedule whether or
// not earlier ones were answered, and latency is measured from the scheduled
// time, so a stalled relay shows up as latency rather than as fewer queries.
// Otherwise it is closed loop: Concurrency workers each keep one query (or one
// pipelined pair) outstanding, which measures capacity.
type load struct {
	// Name labels the load in the report.
	Name string
	// Sandbox indexes the scenario's sandboxes.
	Sandbox int
	Proto   proto

	// QPS is the open-loop query rate. Zero selects closed loop.
	QPS float64
	// Concurrency is the number of closed-loop workers, or the most queries
	// an open-loop load keeps outstanding (default 4096). Sends beyond it are
	// counted as Skipped.
	Concurrency int

	// Timeout is how long a client waits for an answer (default 5s, glibc's).
	Timeout time.Duration
	// QTypes are cycled through (default A only).
	QTypes []uint16

	// TCP only. Pipeline sends A and AAAA back to back on one connection, as
	// glibc does with "options use-vc". ReuseConn keeps a closed-loop
	// worker's connection for its next query instead of dialing per query.
	Pipeline  bool
	ReuseConn bool

	// IdleConns opens this many TCP connections that send nothing, holding
	// relay connection slots for the length of the load.
	IdleConns int

	// StartAfter and StopAfter bound the load within the scenario, measured
	// from its start. A zero StopAfter runs to the end.
	StartAfter time.Duration
	StopAfter  time.Duration
}

// loadStats is what a [load] observed.
type loadStats struct {
	Sent      int64
	Answered  int64
	Rcodes    map[string]int64
	Timeouts  int64 // no answer within Timeout
	Malformed int64 // answer failed validation
	Errors    map[string]int64
	Skipped   int64 // open-loop sends dropped because Concurrency was reached
	IdleHeld  int64 // idle connections the relay accepted and kept open
	Latency   *histogram
}

// answeredFraction is answered / sent, or 1 when nothing was sent.
func (s *loadStats) answeredFraction() float64 {
	if s.Sent == 0 {
		return 1
	}
	return float64(s.Answered) / float64(s.Sent)
}

// loadRecorder accumulates stats concurrently.
type loadRecorder struct {
	sent, answered, timeouts, malformed, skipped, idleHeld atomic.Int64
	latency                                                histogram
	mu                                                     sync.Mutex
	rcodes, errors                                         map[string]int64
}

func newLoadRecorder() *loadRecorder {
	return &loadRecorder{rcodes: map[string]int64{}, errors: map[string]int64{}}
}

func (r *loadRecorder) answer(rcode int, latency time.Duration) {
	r.answered.Add(1)
	r.latency.record(latency)
	r.mu.Lock()
	r.rcodes[rcodeName(rcode)]++
	r.mu.Unlock()
}

func (r *loadRecorder) fail(err error) {
	if isTimeout(err) {
		r.timeouts.Add(1)
		return
	}
	r.mu.Lock()
	r.errors[errorKind(err)]++
	r.mu.Unlock()
}

func (r *loadRecorder) stats() loadStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := loadStats{
		Sent:      r.sent.Load(),
		Answered:  r.answered.Load(),
		Rcodes:    map[string]int64{},
		Timeouts:  r.timeouts.Load(),
		Malformed: r.malformed.Load(),
		Errors:    map[string]int64{},
		Skipped:   r.skipped.Load(),
		IdleHeld:  r.idleHeld.Load(),
		Latency:   &r.latency,
	}
	for k, v := range r.rcodes {
		s.Rcodes[k] = v
	}
	for k, v := range r.errors {
		s.Errors[k] = v
	}
	return s
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout() || errors.Is(err, os.ErrDeadlineExceeded)
}

func errorKind(err error) string {
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	case errors.Is(err, syscall.ECONNRESET):
		return "reset"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	case errors.Is(err, net.ErrClosed):
		return "closed"
	}
	return "other"
}

// endpoint is where a sandbox's relay listens.
type endpoint struct {
	udp string
	tcp string
}

// queryNames are spread over so that answers are not all identical.
var queryNames = []string{"example.com", "www.example.com", "api.example.net", "svc.cluster.local", "a.b.example.org"}

// udpBufs hold answer buffers. Larger answers are read truncated, which is
// enough to validate them; a smaller buffer keeps the client's share of the
// heap measurements down.
var udpBufs = sync.Pool{New: func() any { b := make([]byte, 8192); return &b }}

type client struct {
	load load
	ep   endpoint
	rec  *loadRecorder
	seq  atomic.Uint64
}

// runLoad sends load to ep until ctx is done, then waits for outstanding
// queries to be answered or time out.
func runLoad(ctx context.Context, load load, ep endpoint) loadStats {
	if load.Timeout <= 0 {
		load.Timeout = 5 * time.Second
	}
	if len(load.QTypes) == 0 {
		load.QTypes = []uint16{typeA}
	}
	c := &client{load: load, ep: ep, rec: newLoadRecorder()}

	var wg sync.WaitGroup
	for range load.IdleConns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.holdIdle(ctx)
		}()
	}
	switch {
	case load.QPS > 0:
		c.openLoop(ctx, &wg)
	case load.Concurrency > 0:
		for range load.Concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.closedLoop(ctx)
			}()
		}
	}
	wg.Wait()
	return c.rec.stats()
}

// nextQueries returns the queries for one exchange: one, or an A+AAAA pair
// when pipelining.
func (c *client) nextQueries() [][]byte {
	n := c.seq.Add(1)
	name := queryNames[n%uint64(len(queryNames))]
	if c.load.Proto == protoTCP && c.load.Pipeline {
		// Distinct IDs: answers on a connection are matched by ID.
		id := uint16(rand.Uint32())
		return [][]byte{
			buildQuery(id, name, typeA),
			buildQuery(id+1, name, typeAAAA),
		}
	}
	return [][]byte{buildQuery(uint16(rand.Uint32()), name, c.load.QTypes[n%uint64(len(c.load.QTypes))])}
}

func (c *client) openLoop(ctx context.Context, wg *sync.WaitGroup) {
	limit := c.load.Concurrency
	if limit <= 0 {
		limit = 4096
	}
	sem := make(chan struct{}, limit)
	interval := time.Duration(float64(time.Second) / c.load.QPS)
	start := time.Now()
	for i := 0; ; i++ {
		// Uniform schedule from a fixed origin: a late tick does not shift
		// the ones after it.
		scheduled := start.Add(time.Duration(i) * interval)
		if wait := time.Until(scheduled); wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		} else if ctx.Err() != nil {
			return
		}
		select {
		case sem <- struct{}{}:
		default:
			c.rec.skipped.Add(1)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			c.exchange(nil, scheduled)
		}()
	}
}

func (c *client) closedLoop(ctx context.Context) {
	var conn net.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	for ctx.Err() == nil {
		if c.load.Proto == protoTCP && c.load.ReuseConn {
			if conn == nil {
				var err error
				if conn, err = net.DialTimeout("tcp", c.ep.tcp, c.load.Timeout); err != nil {
					c.rec.sent.Add(1)
					c.rec.fail(err)
					sleepCtx(ctx, 10*time.Millisecond)
					continue
				}
			}
			if !c.exchange(conn, time.Now()) {
				_ = conn.Close()
				conn = nil
			}
			continue
		}
		if !c.exchange(nil, time.Now()) {
			// Refusals return at once; don't spin on a closed relay.
			sleepCtx(ctx, time.Millisecond)
		}
	}
}

// exchange sends one round of queries and waits for the answers. A nil conn
// dials a new one and closes it afterwards. It reports whether conn is still
// usable.
func (c *client) exchange(conn net.Conn, start time.Time) bool {
	queries := c.nextQueries()
	c.rec.sent.Add(int64(len(queries)))
	deadline := start.Add(c.load.Timeout)
	if conn == nil {
		network, addr := "udp", c.ep.udp
		if c.load.Proto == protoTCP {
			network, addr = "tcp", c.ep.tcp
		}
		d := net.Dialer{Deadline: deadline}
		var err error
		if conn, err = d.Dial(network, addr); err != nil {
			for range queries {
				c.rec.fail(err)
			}
			return false
		}
		defer conn.Close()
	}
	_ = conn.SetDeadline(deadline)
	if c.load.Proto == protoUDP {
		return c.exchangeUDP(conn, queries[0], start)
	}
	return c.exchangeTCP(conn, queries, start)
}

func (c *client) exchangeUDP(conn net.Conn, query []byte, start time.Time) bool {
	if _, err := conn.Write(query); err != nil {
		c.rec.fail(err)
		return false
	}
	bufp := udpBufs.Get().(*[]byte)
	defer udpBufs.Put(bufp)
	buf := *bufp
	for {
		n, err := conn.Read(buf)
		if err != nil {
			c.rec.fail(err)
			return false
		}
		// A connected socket only hears from the relay; anything with
		// another ID is a late answer to someone else and is skipped, as
		// stub resolvers do.
		if n >= 2 && messageID(buf[:n]) != messageID(query) {
			continue
		}
		c.record(query, buf[:n], start)
		return true
	}
}

func (c *client) exchangeTCP(conn net.Conn, queries [][]byte, start time.Time) bool {
	var out []byte
	for _, q := range queries {
		out = binary.BigEndian.AppendUint16(out, uint16(len(q)))
		out = append(out, q...)
	}
	if _, err := conn.Write(out); err != nil {
		for range queries {
			c.rec.fail(err)
		}
		return false
	}
	// Answers may come back in any order (RFC 7766 §7); match them by ID.
	pending := map[uint16][]byte{}
	for _, q := range queries {
		pending[messageID(q)] = q
	}
	for len(pending) > 0 {
		var prefix [2]byte
		if _, err := io.ReadFull(conn, prefix[:]); err != nil {
			for range pending {
				c.rec.fail(err)
			}
			return false
		}
		resp := make([]byte, binary.BigEndian.Uint16(prefix[:]))
		if _, err := io.ReadFull(conn, resp); err != nil {
			for range pending {
				c.rec.fail(err)
			}
			return false
		}
		if len(resp) < 2 {
			c.rec.malformed.Add(1)
			continue
		}
		q, ok := pending[messageID(resp)]
		if !ok {
			c.rec.malformed.Add(1)
			continue
		}
		delete(pending, messageID(resp))
		c.record(q, resp, start)
	}
	return true
}

func (c *client) record(query, resp []byte, start time.Time) {
	rcode, err := checkAnswer(query, resp)
	if err != nil {
		c.rec.malformed.Add(1)
		return
	}
	c.rec.answer(rcode, time.Since(start))
}

// holdIdle keeps a TCP connection open without sending until ctx is done. It
// redials when the relay closes it, as an idle-pinning client would.
func (c *client) holdIdle(ctx context.Context) {
	for ctx.Err() == nil {
		conn, err := net.DialTimeout("tcp", c.ep.tcp, time.Second)
		if err != nil {
			c.rec.fail(fmt.Errorf("idle: %w", err))
			sleepCtx(ctx, 50*time.Millisecond)
			continue
		}
		closed := make(chan struct{})
		go func() {
			// A read returns when the relay closes the connection.
			_, _ = conn.Read(make([]byte, 1))
			close(closed)
		}()
		select {
		case <-closed:
			// Refused: the relay accepts, then closes at once.
			c.rec.fail(io.EOF)
			_ = conn.Close()
			sleepCtx(ctx, 50*time.Millisecond)
			continue
		case <-time.After(100 * time.Millisecond):
			c.rec.idleHeld.Add(1)
		case <-ctx.Done():
		}
		select {
		case <-closed:
		case <-ctx.Done():
		}
		_ = conn.Close()
		<-closed
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
