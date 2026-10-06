# Actor DNS

Actor DNS relays DNS requests from the actor to the external world.

## Code structure

### Relay

- `Relay` -- one per Ateom, holds upstream resolvers and worker-wide concurrency limits, and is used to create the per-Actor `Server`.
- `Relay.Serve()` -- creates a per-Actor `Server` that manages DNS requests for the Actor. Returns a `*Server`.

### Server

`Server` is the per-Actor state for the DNS traffic.

- `Server.pendingRequests` map:
  - Allocates a rewritten `upstreamID` (`uint16`) per in-flight query to avoid transaction ID collisions across different actor source ports on the shared egress socket (and prevent predictable upstream IDs).
  - Maps `(upstreamID, transportSource)` -> pending request entry:
    - `clientRequestID` (`uint16`) and `clientSource` (actor UDP addr or TCP connection) to restore the original ID and route the response back.
    - Question metadata (`QNAME`, `QTYPE`, `QCLASS`) to validate that the response matches the query (RFC 5452).
    - `upstreamIdx` and `deferredResp` to track multi-upstream failover (`SERVFAIL`, `NOTIMP`, `REFUSED`, or timeout) and return the last failure response if all upstreams fail.
    - `expiry` timestamp for timing out stale entries and triggering failover via a periodic sweep ticker.
- `Server.Stop()` -- cancels in-flight work, closes sockets, and stops serving DNS requests.

### Implementation details

#### UDP handling

- `udphandler.go`: `struct udpHandler`
- Two goroutines per `Server` (plus a sweep ticker for timeouts/failover):
  - **Ingress reader**: reads datagrams from the actor-facing gateway `net.PacketConn`, runs `onRequest()`, rewrites the transaction ID to `upstreamID`, records the entry in `pendingRequests`, and sends the query on the egress socket.
  - **Egress reader**: reads datagrams from an unconnected worker-namespace `net.PacketConn`, runs `onResponse()` to match and validate against `pendingRequests`, either fails over to the next upstream or restores `clientRequestID` and writes the answer back to `clientSource`.

#### TCP handling

- `tcphandler.go`: `struct tcpHandler`
- Framing: each DNS message on a TCP stream is prefixed with a 2-byte big-endian length (`uint16`, RFC 1035 §4.2.2).
- Pipelining and out-of-order responses (RFC 7766 §6.2.1.1):
  - Two goroutines per accepted TCP connection sharing a per-connection downstream write mutex:
    - **Downstream reader**: loops reading length-prefixed frames from the actor connection and calls `onRequest()`.
      - If forwarded: records the request in `pendingRequests` and writes the frame to the upstream TCP connection.
      - If rejected with a synthesized reply (`FORMERR`, `NOTIMP`, `REFUSED`): writes the framed error response directly to the actor connection under the write mutex without contacting upstream.
    - **Upstream reader**: loops reading length-prefixed frames from the upstream TCP connection, validates and matches each frame via `onResponse()`, and writes valid responses back to the actor connection under the write mutex.

#### DNS protocol layer

`dnshandler.go`: `struct dnsHandler`

UDP and TCP both call up to the DNS protocol layer (`golang.org/x/net/dns/dnsmessage`), which inspects packets and returns transport-agnostic decisions:

- `onRequest(raw []byte)` -> `Action`:
  - **Drop**: packet `< 12` bytes, `QR == 1` (response bit set on a query), or rate-limited.
  - **Reply** (synthesized response without upstream round-trip):
    - `FORMERR` (`RCode = 1`): malformed header/question or `QDCOUNT != 1`.
    - `NOTIMP` (`RCode = 4`): `OpCode != 0` (non-standard query) or zone transfer `QTYPE` (`AXFR`, `IXFR`).
    - `REFUSED` (`RCode = 5`): disallowed by domain or record-type policy.
  - **Forward**: valid query allowed by policy and rate limits; returns parsed question metadata for `pendingRequests`.
- `onResponse(raw []byte, from net.Addr)` -> `Action`:
  - Validates `QR == 1` and parses header + question.
  - Matches against `pendingRequests` (verifying `upstreamID`, expected upstream source, and matching `QNAME`/`QTYPE`/`QCLASS`).
  - **Drop**: unknown/unmatched responses or mismatched question section.
  - **Failover**: on `SERVFAIL`, `NOTIMP`, or `REFUSED` when additional upstreams remain, retains `deferredResp` and retries the next upstream.
  - **Deliver**: restores `clientRequestID` and returns the target `clientSource` to write the response to.
