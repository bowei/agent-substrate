# Debian Network Debugging Demo

This demo deploys an on-demand, sandboxed Debian actor equipped with network diagnostics and troubleshooting utilities (`iproute2`, `iputils-ping`, `dnsutils`, `curl`, `wget`, `tcpdump`, `traceroute`, `mtr`, `iperf3`, `socat`, `netcat-openbsd`).

The actor exposes a shell interface over HTTP using [`httpcmd`](https://github.com/haccht/httpcmd), enabling clients to run network diagnostics against the actor's sandboxed network namespace directly via `curl` requests routed through the Substrate router.

---

## What it demonstrates

```text
Client (curl)
    │
    │  Host: netdebug-1.debug.actors.resources.substrate.ate.dev
    ▼
[atenet Router (port 8000)]
    │  (Wakes/resumes actor if suspended, proxies HTTP traffic)
    ▼
[ateom Worker Pod (gVisor sandbox)]
    │
    ▼
[Container: Debian + httpcmd (port 80)]
    │  Executes: /bin/bash -c "<command from ?arg=...>"
    ▼
[Sandboxed Network Stack: ip, ping, dig, traceroute, curl, etc.]
```

1. **Sandboxed Network Environment**: Each actor runs in an isolated gVisor network namespace with its own IP and interface configuration.
2. **HTTP-Wrapped CLI Execution**: Using `httpcmd --permit-argument bash -c`, any shell command can be sent as URL query arguments and its output streams back over HTTP.
3. **Substrate Uniform Routing**: The atenet router proxies traffic to the actor using uniform DNS naming (`<actor-name>.<atespace>.actors.resources.substrate.ate.dev`).
4. **Stateful Suspend & Resume**: The actor can be suspended when idle to free physical worker pod capacity, and instantly resumes upon the next HTTP request.

---

## Prerequisites

- A Kubernetes cluster with Agent Substrate installed:
  - **Local Kind**: `./hack/create-kind-cluster.sh` then `./hack/install-ate-kind.sh --deploy-ate-system`
  - **GKE**: `./hack/install-ate.sh --deploy-ate-system`
- `ko`, `kubectl`, `docker`, and `kubectl-ate` (`go install ./cmd/kubectl-ate`).
- Environment variables: `BUCKET_NAME` (e.g. `ate-snapshots` for Kind) and `KO_DOCKER_REPO` (e.g. `localhost:5001` for Kind).

---

## How to Run on Agent Substrate

### 1. Build and Deploy

Use the core installation script to build the workload container and apply the resolved manifests:

```bash
# On local Kind:
./hack/install-ate-kind.sh --deploy-demo-netdebug

# On GKE:
./hack/install-ate.sh --deploy-demo-netdebug
```

This command will:
- Build the Debian + `httpcmd` container image via `docker buildx` and push it to `${KO_DOCKER_REPO}`.
- Create the `ate-demo-netdebug` namespace.
- Create the `netdebug` `WorkerPool` and `ActorTemplate`.
- Wait until the WorkerPool is rolled out and the ActorTemplate is `Ready`.

---

### 2. Create a Network Debugger Actor

Actors live inside an **atespace**. Create an atespace (e.g. `debug`), then spawn a debugger actor instance (e.g. `diag-1`):

```bash
# Ensure kubectl-ate is installed
go install ./cmd/kubectl-ate

# Create atespace
kubectl ate create atespace debug

# Create actor from the netdebug template
kubectl ate create actor diag-1 -a debug --template ate-demo-netdebug/netdebug
```

---

### 3. Port-Forward the Atenet Router

```bash
kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

---

## Running Network Diagnostics

Send HTTP requests with `?arg=...` query parameters to execute commands inside the actor.

### Inspect Network Interfaces & Routing Table
```bash
# Check IP addresses inside the sandbox
curl -sN "http://localhost:8000/?arg=ip%20addr" \
  -H "Host: diag-1.debug.actors.resources.substrate.ate.dev"

# Check routing table
curl -sN "http://localhost:8000/?arg=ip%20route" \
  -H "Host: diag-1.debug.actors.resources.substrate.ate.dev"
```

### DNS Resolution Diagnostics
```bash
# Query DNS records with dig
curl -sN "http://localhost:8000/?arg=dig&arg=+short&arg=google.com" \
  -H "Host: diag-1.debug.actors.resources.substrate.ate.dev"

# Inspect /etc/resolv.conf
curl -sN "http://localhost:8000/?arg=cat&arg=/etc/resolv.conf" \
  -H "Host: diag-1.debug.actors.resources.substrate.ate.dev"
```

### ICMP Ping & Traceroute
```bash
# Ping 8.8.8.8 (3 packets)
curl -sN "http://localhost:8000/?arg=ping&arg=-c3&arg=8.8.8.8" \
  -H "Host: diag-1.debug.actors.resources.substrate.ate.dev"

# Traceroute to destination
curl -sN "http://localhost:8000/?arg=traceroute&arg=-n&arg=8.8.8.8" \
  -H "Host: diag-1.debug.actors.resources.substrate.ate.dev"
```

### HTTP Inspection
```bash
# Check HTTP response headers from inside the sandbox
curl -sN "http://localhost:8000/?arg=curl&arg=-sI&arg=https://www.google.com" \
  -H "Host: diag-1.debug.actors.resources.substrate.ate.dev"
```

---

## Suspend & Resume Continuity

1. Check actor status:
   ```bash
   kubectl ate get actor diag-1 -a debug
   ```

2. Suspend the actor (writes checkpoint to GCS snapshot storage and frees the worker pod):
   ```bash
   kubectl ate suspend actor diag-1 -a debug
   ```

3. Send another HTTP request:
   ```bash
   curl -sN "http://localhost:8000/?arg=uptime" \
     -H "Host: diag-1.debug.actors.resources.substrate.ate.dev"
   ```
   The router automatically restores the actor to an available worker and serves the request.

---

## Teardown and Cleanup

1. Remove the actor and atespace:
   ```bash
   kubectl ate delete actor diag-1 -a debug
   kubectl ate delete atespace debug
   ```

2. Delete the demo resources from the cluster:
   ```bash
   ./hack/install-ate.sh --delete-demo-netdebug
   ```
