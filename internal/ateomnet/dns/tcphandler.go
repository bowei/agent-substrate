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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// tcpClientConn wraps a downstream actor TCP connection with a mutex to
// serialize concurrent frame writes from synthesized replies and out-of-order
// upstream responses.
type tcpClientConn struct {
	conn    net.Conn
	writeMu sync.Mutex
	eof     atomic.Bool
}

func (c *tcpClientConn) writeFrame(msg []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return writeTCPFrame(c.conn, msg)
}

// readTCPFrame reads a single RFC 1035 §4.2.2 2-byte big-endian length-prefixed
// DNS message from r.
func readTCPFrame(r io.Reader) ([]byte, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint16(lenBuf[:]))
	msg := make([]byte, length)
	if _, err := io.ReadFull(r, msg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return msg, nil
}

// writeTCPFrame writes msg prefixed with its 2-byte big-endian length to w.
func writeTCPFrame(w io.Writer, msg []byte) error {
	if len(msg) > 0xffff {
		return fmt.Errorf("dns: TCP message length %d exceeds uint16 maximum", len(msg))
	}
	frame := make([]byte, 2+len(msg))
	binary.BigEndian.PutUint16(frame[0:2], uint16(len(msg)))
	copy(frame[2:], msg)
	_, err := w.Write(frame)
	return err
}

// tcpHandler accepts actor TCP DNS connections and relays length-prefixed DNS
// frames with pipelining and out-of-order response support.
type tcpHandler struct {
	listener  net.Listener
	dialer    net.Dialer
	upstreams []string
	dns       packetHandler
	pending   *pendingRequests
	limiter   *limiter

	tcpTimeout time.Duration
}

func newTCPHandler(
	listener net.Listener,
	dialer net.Dialer,
	upstreams []string,
	dns packetHandler,
	pending *pendingRequests,
	lim *limiter,
) *tcpHandler {
	return &tcpHandler{
		listener:   listener,
		dialer:     dialer,
		upstreams:  upstreams,
		dns:        dns,
		pending:    pending,
		limiter:    lim,
		tcpTimeout: dnsTCPTimeout,
	}
}

// serve accepts and relays actor TCP DNS connections until ctx is canceled or
// the listener closes.
func (h *tcpHandler) serve(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := h.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("dns: accepting actor DNS connection: %w", err)
		}
		if h.limiter != nil {
			select {
			case h.limiter.connections <- struct{}{}:
			default:
				slog.DebugContext(ctx, "dns relay refused a DNS connection; too many open")
				_ = conn.Close()
				continue
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if h.limiter != nil {
				defer func() { <-h.limiter.connections }()
			}
			h.handleConn(ctx, conn)
		}()
	}
}

func (h *tcpHandler) handleConn(ctx context.Context, downstream net.Conn) {
	defer downstream.Close()

	var upstream net.Conn
	var errs error
	for _, address := range h.upstreams {
		conn, err := h.dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		upstream = conn
		break
	}
	if upstream == nil {
		slog.WarnContext(ctx, "dns relay could not reach any resolver for an actor DNS connection", slog.Any("err", errs))
		return
	}
	defer upstream.Close()

	stop := context.AfterFunc(ctx, func() {
		_ = downstream.Close()
		_ = upstream.Close()
	})
	defer stop()

	tcpTimeout := h.tcpTimeout
	if tcpTimeout <= 0 {
		tcpTimeout = dnsTCPTimeout
	}
	deadline := time.Now().Add(tcpTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = downstream.SetDeadline(deadline)
	_ = upstream.SetDeadline(deadline)

	client := &tcpClientConn{conn: downstream}
	defer h.pending.removeByClientSource(client)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		h.readDownstream(ctx, client, upstream)
	}()
	go func() {
		defer wg.Done()
		h.readUpstream(ctx, client, upstream)
	}()
	wg.Wait()
}

func (h *tcpHandler) readDownstream(ctx context.Context, client *tcpClientConn, upstream net.Conn) {
	upstreamSource := normalizeAddr(upstream.RemoteAddr())
	upstreams := []string{upstreamSource}

	for {
		raw, err := readTCPFrame(client.conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				client.eof.Store(true)
				if c, ok := upstream.(*net.TCPConn); ok {
					_ = c.CloseWrite()
				}
				if h.pending.countByClientSource(client) == 0 {
					_ = upstream.Close()
				}
				return
			}
			_ = upstream.Close()
			_ = client.conn.Close()
			return
		}

		act := h.dns.onRequest(raw)
		switch act.kind {
		case actionDrop:
			continue
		case actionReply:
			if err := client.writeFrame(act.payload); err != nil {
				if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
					slog.DebugContext(ctx, "dns relay could not write synthesized TCP reply", slog.Any("err", err))
				}
				_ = upstream.Close()
				_ = client.conn.Close()
				return
			}
		case actionForward:
			_, ok := h.pending.record(raw, act.clientRequestID, act.question, client, upstreams, act.hasSlot)
			if !ok {
				continue
			}
			if err := writeTCPFrame(upstream, raw); err != nil {
				if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
					slog.WarnContext(ctx, "dns relay could not forward TCP DNS query", slog.Any("err", err))
				}
				_ = upstream.Close()
				_ = client.conn.Close()
				return
			}
		}
	}
}

func (h *tcpHandler) readUpstream(ctx context.Context, client *tcpClientConn, upstream net.Conn) {
	from := upstream.RemoteAddr()
	for {
		raw, err := readTCPFrame(upstream)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if c, ok := client.conn.(*net.TCPConn); ok {
					_ = c.CloseWrite()
				}
			}
			_ = client.conn.Close()
			return
		}

		act := h.dns.onResponse(raw, from)
		switch act.kind {
		case actionDrop:
			continue
		case actionDeliver:
			target, ok := act.clientSource.(*tcpClientConn)
			if !ok {
				continue
			}
			if err := target.writeFrame(act.payload); err != nil {
				if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
					slog.WarnContext(ctx, "dns relay could not return TCP DNS answer", slog.Any("err", err))
				}
				_ = upstream.Close()
				_ = client.conn.Close()
				return
			}
			if client.eof.Load() && h.pending.countByClientSource(client) == 0 {
				_ = upstream.Close()
				return
			}
		}
	}
}
