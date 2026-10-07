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

package dns

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"
)

func startTestTCPHandler(t *testing.T, upstreams []string, configure func(*tcpHandler, *stubPacketHandler)) net.Addr {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	lim := &limiter{
		inFlight:    make(chan struct{}, maxInFlightDNS),
		connections: make(chan struct{}, maxDNSConnections),
	}
	pending := newPendingRequests(lim)
	stub := &stubPacketHandler{pending: pending, lim: lim}
	h := newTCPHandler(lis, net.Dialer{Timeout: 5 * time.Second}, upstreams, stub, pending, lim)
	if configure != nil {
		configure(h, stub)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.serve(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = lis.Close()
		<-done
	})

	return lis.Addr()
}

// newOutOfOrderTCPResolver reads 2 queries on a single TCP connection and
// answers the second query before the first to exercise RFC 7766 pipelining.
func newOutOfOrderTCPResolver(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				q1, err := readTCPFrame(conn)
				if err != nil {
					return
				}
				q2, err := readTCPFrame(conn)
				if err != nil {
					return
				}
				// Respond to q2 first, then q1.
				_ = writeTCPFrame(conn, dnsAnswer(q2, 0))
				_ = writeTCPFrame(conn, dnsAnswer(q1, 0))
			}()
		}
	}()
	return lis.Addr().String()
}

func TestTCPHandlerPipeliningAndOutOfOrderResponses(t *testing.T) {
	upstream := newOutOfOrderTCPResolver(t)
	addr := startTestTCPHandler(t, []string{upstream}, nil)

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	if err := writeTCPFrame(conn, dnsQuery(0x1111)); err != nil {
		t.Fatal(err)
	}
	if err := writeTCPFrame(conn, dnsQuery(0x2222)); err != nil {
		t.Fatal(err)
	}

	resp1, err := readTCPFrame(conn)
	if err != nil {
		t.Fatalf("reading first response frame: %v", err)
	}
	resp2, err := readTCPFrame(conn)
	if err != nil {
		t.Fatalf("reading second response frame: %v", err)
	}

	if got1 := binary.BigEndian.Uint16(resp1[0:2]); got1 != 0x2222 {
		t.Errorf("first response ID = %#x, want 0x2222 (out-of-order)", got1)
	}
	if got2 := binary.BigEndian.Uint16(resp2[0:2]); got2 != 0x1111 {
		t.Errorf("second response ID = %#x, want 0x1111", got2)
	}
}

func TestTCPHandlerSynthesizedReplyAndHalfClose(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		q, err := readTCPFrame(conn)
		if err != nil {
			return
		}
		_ = writeTCPFrame(conn, dnsAnswer(q, 0))
	}()

	addr := startTestTCPHandler(t, []string{lis.Addr().String()}, func(_ *tcpHandler, stub *stubPacketHandler) {
		stub.onReq = func(raw []byte) (action, bool) {
			if len(raw) >= 2 && binary.BigEndian.Uint16(raw[0:2]) == 0xbad0 {
				return action{
					kind:    actionReply,
					payload: dnsAnswer(raw, rcodeRefused),
				}, true
			}
			return action{}, false
		}
	})

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	// First frame triggers a synthesized reply; second frame is forwarded, and
	// then client half-closes its write side.
	if err := writeTCPFrame(conn, dnsQuery(0xbad0)); err != nil {
		t.Fatal(err)
	}
	if err := writeTCPFrame(conn, dnsQuery(0x1234)); err != nil {
		t.Fatal(err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}

	r1, err := readTCPFrame(conn)
	if err != nil {
		t.Fatalf("reading first frame: %v", err)
	}
	r2, err := readTCPFrame(conn)
	if err != nil {
		t.Fatalf("reading second frame: %v", err)
	}

	got := map[uint16]byte{
		binary.BigEndian.Uint16(r1[0:2]): r1[3] & 0x0f,
		binary.BigEndian.Uint16(r2[0:2]): r2[3] & 0x0f,
	}
	if got[0xbad0] != rcodeRefused {
		t.Errorf("0xbad0 rcode = %d, want %d", got[0xbad0], rcodeRefused)
	}
	if got[0x1234] != 0 {
		t.Errorf("0x1234 rcode = %d, want 0", got[0x1234])
	}
	wg.Wait()
}
