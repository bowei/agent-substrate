---
name: create-demo
description: Guide for creating, packaging, deploying, and verifying custom demos in Agent Substrate given a Go binary, custom script, or containerized workload.
---

# Creating Custom Demos in Agent Substrate

This skill provides step-by-step instructions for creating a custom demo application on Agent Substrate given a Go binary, custom script (Python, Bash, Node.js), or containerized workload.

---

## 1. Overview & Architecture

An Agent Substrate demo typically demonstrates multiplexing, suspend/resume continuity, stateful durability, request routing/parking, or sandboxed execution.

A complete demo consists of four key components:

```text
demos/<demo-name>/
├── README.md                      # Walkthrough, architecture, and verification steps
├── <demo-name>.yaml.tmpl          # Manifest template (Namespace, WorkerPool, ActorTemplate)
├── <workload files>               # Go source files OR Dockerfile + entrypoint script
└── (optional) test scripts        # Automated verification or load generation scripts

hack/
├── install-demo-<demo-name>.sh    # Hook into hack/install-ate.sh deployment lifecycle
```

### Resource Hierarchy

```
                    ┌─────────────────────────┐
                    │ Namespace: ate-demo-... │
                    └───────────┬─────────────┘
                                │
        ┌───────────────────────┴───────────────────────┐
        ▼                                               ▼
┌───────────────────────────────┐     ┌───────────────────────────────────┐
│ WorkerPool                    │     │ ActorTemplate                     │
│  • Standby pod capacity       │     │  • Workload container image       │
│  • ateom supervisor           │◄────┤  • workerSelector (matches pool)  │
│  • sandboxClass (gvisor/etc)  │     │  • snapshotsConfig (GCS bucket)   │
└───────────────────────────────┘     │  • readyz HTTP probe (port 80)    │
                                      │  • volumeMounts (optional durable)│
                                      └─────────────────┬─────────────────┘
                                                        │
                                    ┌───────────────────┴───────────────────┐
                                    ▼                                       ▼
                       ┌────────────────────────┐              ┌────────────────────────┐
                       │ Actor: my-actor-1      │              │ Actor: my-actor-2      │
                       │ (atespace: demo)       │              │ (atespace: demo)       │
                       └────────────────────────┘              └────────────────────────┘
```

---

## 2. Choosing a Packaging Strategy

| Workload Type | Build Tool | Image Specification in Manifest | Typical Use Case |
| :--- | :--- | :--- | :--- |
| **Go Binary** | `ko` | `ko://github.com/agent-substrate/substrate/demos/<demo-name>` | High-performance Go microservices, stateful HTTP servers (`counter`, `egress`) |
| **Script / Non-Go / Custom Runtime** | `docker buildx` | `${WORKLOAD_IMAGE}` (digest-resolved at deploy time) | Python/Node/Bash agents, external CLI tools (`claude-code-multiplex`) |

---

## 3. Step-by-Step Implementation Workflow

### Step 1: Implement the Workload

#### Option A: Go HTTP Service (Built with `ko`)

Place Go code directly under `demos/<demo-name>/` (e.g., `main.go`).

**Requirements & Best Practices:**
1. **HTTP Server & Uniform DNS**: Listen on port `80` (or `PORT` env var). Substrate routes incoming requests matching `<actor-name>.<atespace>.actors.resources.substrate.ate.dev` to the actor container.
2. **Readiness Probe (`/readyz`)**: Expose an HTTP endpoint returning `200 OK` once initialization is complete. This allows Substrate to skip the default ~20s golden snapshot warmup delay.
3. **Actor Identity**: Read `/run/ate/actor-id` dynamically at request time (Substrate bind-mounts this file per actor). Avoid caching at startup because environment variables and startup state are frozen into the golden snapshot.
4. **State Persistence**:
   - **In-Memory**: Kept across suspend/resume automatically via gVisor process memory checkpoints.
   - **Filesystem**: Store mutable data in a directory declared as a `durableDir` volume.

*Example Go workload structure:*
```go
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "80"
	}

	http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		actorID, _ := os.ReadFile("/run/ate/actor-id")
		fmt.Fprintf(w, "Hello from actor: %s\n", string(actorID))
	})

	log.Printf("Serving on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
```

#### Option B: Script / Custom Environment (Built with `Dockerfile`)

For Python, Node.js, or shell scripts, create a subfolder `demos/<demo-name>/workload/`.

1. **`demos/<demo-name>/workload/Dockerfile`**:
   ```dockerfile
   FROM python:3.11-slim
   RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*
   COPY run.py /run.py
   ENTRYPOINT ["python", "-u", "/run.py"]
   ```

2. **`demos/<demo-name>/workload/run.py` (Idle/Multiplex Pattern)**:
   For long-running background agents, implement periodic task execution interspersed with idle `sleep` windows so Substrate can suspend idle actors and multiplex worker pods.

---

### Step 2: Create the Manifest Template (`demos/<demo-name>/<demo-name>.yaml.tmpl`)

Create the template with `${BUCKET_NAME}` (and `${WORKLOAD_IMAGE}` if using Dockerfile).

```yaml
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

apiVersion: v1
kind: Namespace
metadata:
  name: ate-demo-<demo-name>

---

apiVersion: ate.dev/v1alpha1
kind: WorkerPool
metadata:
  name: <demo-name>
  namespace: ate-demo-<demo-name>
  labels:
    workload: <demo-name>
spec:
  replicas: 2
  ateomImage: ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor

---

apiVersion: ate.dev/v1alpha1
kind: ActorTemplate
metadata:
  name: <demo-name>
  namespace: ate-demo-<demo-name>
spec:
  containers:
  - name: <demo-name>
    # Use ko URI for Go, or ${WORKLOAD_IMAGE} for Dockerfile
    image: ko://github.com/agent-substrate/substrate/demos/<demo-name>
    readyz:
      httpGet:
        path: /readyz
        port: 80
    volumeMounts:
    - name: data
      mountPath: /data
  workerSelector:
    matchLabels:
      workload: <demo-name>
  snapshotsConfig:
    onPause: Full
    onCommit: Data
    location: gs://${BUCKET_NAME}/ate-demo-<demo-name>/
  volumes:
  - name: data
    durableDir: {}
```

---

### Step 3: Create the Install Script (`hack/install-demo-<demo-name>.sh`)

Create `hack/install-demo-<demo-name>.sh` conforming to the `install-ate.sh` hook contract:

```bash
#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This is sourced as part of install-ate.sh. Do not run directly.

ATE_DEMOS+=(demo-<demo-name>) # register demo-<demo-name>

demo-<demo-name>_cmdline() {
  case "${1}" in
    --deploy-demo-<demo-name>) demo-<demo-name>_deploy ;;
    --delete-demo-<demo-name>) demo-<demo-name>_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-<demo-name>_deploy() {
  log_step "demo-<demo-name>_deploy"
  ensure_crds

  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      demos/<demo-name>/<demo-name>.yaml.tmpl \
    | run_ko apply -f -

  log_step "Waiting for <demo-name> demo to be ready..."
  run_kubectl rollout status deployment/<demo-name> -n ate-demo-<demo-name> --timeout=300s
  run_kubectl wait --for=condition=Ready actortemplate/<demo-name> -n ate-demo-<demo-name> --timeout=300s
}

demo-<demo-name>_delete() {
  log_step "demo-<demo-name>_delete"
  delete_demo_actors ate-demo-<demo-name> <demo-name>
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME:-placeholder}|g" \
      demos/<demo-name>/<demo-name>.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}
```

> [!NOTE]
> If building a Dockerfile workload (Option B), add a helper function `demo-<demo-name>_build_workload()` using `docker buildx build --platform=linux/amd64 --push -t "${repo}:${tag}" ...`, inspect the digest with `jq`, and substitute `${WORKLOAD_IMAGE}` in the deployment function. (See `hack/install-demo-claude-code-multiplex.sh` as reference).

---

### Step 4: Register in `hack/install-ate.sh` and `Makefile`

1. In `hack/install-ate.sh`, add the source line near the top:
   ```bash
   source "${ROOT}"/hack/install-demo-<demo-name>.sh
   ```

2. If the demo is a Go binary, update the `build-demos` target in `Makefile`:
   ```makefile
   .PHONY: build-demos
   build-demos:
       $(KO) build --ldflags="$(LDFLAGS)" ./demos/counter ./demos/egress ./demos/<demo-name>
   ```

---

### Step 5: Write `demos/<demo-name>/README.md`

Every demo must have a clear `README.md` with:
1. **Overview & Concepts Demonstrated**: What feature of Substrate does this demo highlight?
2. **Prerequisites**: Cluster setup and required environment variables (`BUCKET_NAME`, `KO_DOCKER_REPO`, etc.).
3. **Deployment Instructions**:
   - For Local Kind: `hack/install-ate-kind.sh --deploy-demo-<demo-name>`
   - For GKE: `hack/install-ate.sh --deploy-demo-<demo-name>`
4. **Step-by-step Walkthrough**:
   - Creating the atespace: `kubectl ate create atespace <space>`
   - Creating actor instance: `kubectl ate create actor <actor-id> -a <space> --template ate-demo-<demo-name>/<demo-name>`
   - Port-forwarding the router: `kubectl port-forward -n ate-system svc/atenet-router 8000:80`
   - Interacting with the actor: `curl -H "Host: <actor-id>.<space>.actors.resources.substrate.ate.dev" http://localhost:8000/...`
   - Testing suspend & resume: `kubectl ate suspend actor <actor-id> -a <space>`
5. **Teardown Instructions**:
   - Cleaning up actors & atespaces: `kubectl ate delete actor ...`, `kubectl ate delete atespace ...`
   - Deleting demo resources: `hack/install-ate.sh --delete-demo-<demo-name>`

---

## 4. Verification and Testing

### 1. Verification with Local Kind Cluster
```bash
# Ensure local Kind cluster is up
hack/create-kind-cluster.sh

# Deploy Agent Substrate control plane
hack/install-ate-kind.sh --deploy-ate-system

# Deploy the new demo
hack/install-ate-kind.sh --deploy-demo-<demo-name>

# Verify WorkerPool and ActorTemplate are Ready
kubectl get workerpools -n ate-demo-<demo-name>
kubectl get actortemplates -n ate-demo-<demo-name>

# Create atespace and test actor
kubectl ate create atespace test-space
kubectl ate create actor actor-1 -a test-space --template ate-demo-<demo-name>/<demo-name>

# Verify actor runs and responds
kubectl port-forward -n ate-system svc/atenet-router 8000:80 &
PF_PID=$!
curl -i -H "Host: actor-1.test-space.actors.resources.substrate.ate.dev" http://localhost:8000/
kill $PF_PID

# Delete actor and demo
kubectl ate suspend actor actor-1 -a test-space
kubectl ate delete actor actor-1 -a test-space
kubectl ate delete atespace test-space
hack/install-ate-kind.sh --delete-demo-<demo-name>
```

### 2. Code Quality & Format Checks
Before submitting:
```bash
make fmt
make verify
```

---

## 5. Troubleshooting & Checklist

- [ ] **Golden Snapshot Timeout**: Ensure your container exposes a `/readyz` HTTP endpoint on port 80 and returns HTTP 200 promptly.
- [ ] **Actor Deletion Error (`cannot delete actor: unexpected status`)**: Actors must be in `STATUS_SUSPENDED` before `DeleteActor` can be called. `delete_demo_actors` in `hack/install-ate.sh` handles this automatically.
- [ ] **Environment Variables / State in Checkpoint**: Do NOT rely on container startup environment variables for per-actor dynamic state; read `/run/ate/actor-id` dynamically from filesystem.
- [ ] **Sed Variable Substitution**: Ensure all `${...}` placeholders in `<demo-name>.yaml.tmpl` are handled in `demo-<demo-name>_deploy()` and have sensible defaults in `demo-<demo-name>_delete()`.
