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
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet/netns"
)

const (
	// dnsPort is the relay port on the sandbox's gateway.
	dnsPort = 53

	// Read complete UDP datagrams without truncating EDNS responses.
	maxDNSDatagram = 65535

	// Drop excess UDP queries to bound goroutines and upstream sockets.
	maxInFlightDNS = 64

	// maxDNSConnections bounds open TCP connections.
	maxDNSConnections = 16

	// dnsTCPTimeout limits connection lifetime, including idle clients.
	dnsTCPTimeout = 30 * time.Second

	// dnsExchangeTimeout bounds each upstream attempt.
	dnsExchangeTimeout = 5 * time.Second
)

// Relay forwards UDP and TCP DNS unchanged to the worker pod's resolvers.
// It listens in the sandbox's gateway namespace and dials from the worker's.
// DNS bypasses the egress tunnel and is not checked against egress policy.
type Relay struct {
	upstreams []string
	// dialer reaches upstream resolvers from the worker namespace.
	dialer *net.Dialer

	// Limits are shared across all sandboxes using this relay.
	inFlight    chan struct{}
	connections chan struct{}
}

// NewRelay reads nameservers from resolvConfPath and forwards to each on port 53.
func NewRelay(resolvConfPath string) (*Relay, error) {
	upstreams, err := resolvConfNameservers(resolvConfPath)
	if err != nil {
		return nil, err
	}
	return NewRelayForUpstreams(upstreams)
}

// NewRelayForUpstreams forwards to upstreams, each "host:port", tried in order.
func NewRelayForUpstreams(upstreams []string) (*Relay, error) {
	if len(upstreams) == 0 {
		return nil, fmt.Errorf("dns: at least one upstream resolver is required")
	}
	for _, u := range upstreams {
		if _, _, err := net.SplitHostPort(u); err != nil {
			return nil, fmt.Errorf("dns: invalid upstream resolver %q: %w", u, err)
		}
	}

	slog.Info("DNS relay configured", slog.Any("upstreams", upstreams))

	return &Relay{
		upstreams:   upstreams,
		dialer:      &net.Dialer{Timeout: dnsExchangeTimeout},
		inFlight:    make(chan struct{}, maxInFlightDNS),
		connections: make(chan struct{}, maxDNSConnections),
	}, nil
}

// Serve serves UDP and TCP DNS in the sandbox's local gateway namespace.
func (r *Relay) Serve(ctx context.Context, ns netns.Handle) (*Server, error) {
	// Bind the wildcard because the microVM tap's gateway address is added later.
	address := net.JoinHostPort("0.0.0.0", strconv.Itoa(dnsPort))

	var packet net.PacketConn
	var stream net.Listener
	if err := netns.Do(ctx, ns, func(context.Context) error {
		pc, err := net.ListenPacket("udp", address)
		if err != nil {
			return fmt.Errorf("while opening the actor DNS socket: %w", err)
		}
		packet = pc
		l, err := net.Listen("tcp", address)
		if err != nil {
			_ = pc.Close()
			return fmt.Errorf("while opening the actor DNS listener: %w", err)
		}
		stream = l
		return nil
	}); err != nil {
		return nil, err
	}

	return r.ServeOn(ctx, packet, stream), nil
}

// ServeOn serves UDP DNS on packet and TCP DNS on stream, which the caller has
// already bound. This is used for internal testing; it is not intended for
// external callers.
func (r *Relay) ServeOn(ctx context.Context, packet net.PacketConn, stream net.Listener) *Server {
	return newServer(ctx, r, packet, stream)
}
