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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
)

type serverConfig struct {
	upstreams []string
}

type netConn struct {
	dialer      net.Dialer
	udp         net.PacketConn
	egressUDP   net.PacketConn
	tcpListener net.Listener
}

// Server is one sandbox's running DNS relay, returned by [Relay.Serve].
type Server struct {
	n *netConn

	pendingRequests *pendingRequests
	dns             *dnsHandler
	udp             *udpHandler
	tcp             *tcpHandler

	stopServing context.CancelFunc
	closeOnce   sync.Once
	closeErr    error
	serving     sync.WaitGroup
}

func newServer(
	ctx context.Context,
	config *serverConfig,
	netC *netConn,
	limiter *limiter,
) *Server {
	// Detached from the activation RPC's context but cancelable: the relay's
	// capacity is the worker's, so teardown must drop queries still in flight.
	serveCtx, stopServing := context.WithCancel(context.WithoutCancel(ctx))

	if netC.egressUDP == nil {
		egressUDP, err := net.ListenPacket("udp", ":0")
		if err != nil {
			slog.WarnContext(ctx, "Failed to open worker DNS egress socket", slog.Any("err", err))
		} else {
			netC.egressUDP = egressUDP
		}
	}

	udpAddrs := make([]*net.UDPAddr, 0, len(config.upstreams))
	for _, u := range config.upstreams {
		if addr, err := net.ResolveUDPAddr("udp", u); err == nil {
			udpAddrs = append(udpAddrs, addr)
		}
	}

	pending := newPendingRequests(limiter)
	dnsH := newDNSHandler(pending, limiter, nil)
	udpH := newUDPHandler(netC.udp, netC.egressUDP, udpAddrs, dnsH, pending)
	tcpH := newTCPHandler(netC.tcpListener, netC.dialer, config.upstreams, dnsH, pending, limiter)

	s := &Server{
		stopServing:     stopServing,
		n:               netC,
		pendingRequests: pending,
		dns:             dnsH,
		udp:             udpH,
		tcp:             tcpH,
	}
	s.serving.Add(2)
	go func() {
		defer s.serving.Done()
		if err := s.udp.serve(serveCtx); err != nil {
			slog.WarnContext(ctx, "Actor DNS socket stopped", slog.Any("err", err))
		}
	}()
	go func() {
		defer s.serving.Done()
		if err := s.tcp.serve(serveCtx); err != nil {
			slog.WarnContext(ctx, "Actor DNS listener stopped", slog.Any("err", err))
		}
	}()

	return s
}

// Stop cancels in-flight queries and connections, closes the sockets, and
// waits for the serving goroutines to exit or ctx to expire.
func (s *Server) Stop(ctx context.Context) error {
	s.closeOnce.Do(func() {
		// Cancel first: closing the sockets alone leaves the queries already
		// being resolved holding the relay.
		s.stopServing()
		for _, c := range []io.Closer{s.n.udp, s.n.egressUDP, s.n.tcpListener} {
			if c == nil {
				continue
			}
			if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				s.closeErr = errors.Join(s.closeErr, err)
			}
		}
	})
	stopped := make(chan struct{})
	go func() {
		s.serving.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
		return s.closeErr
	case <-ctx.Done():
		return errors.Join(s.closeErr, fmt.Errorf("dns: waiting for relay to stop: %w", ctx.Err()))
	}
}
