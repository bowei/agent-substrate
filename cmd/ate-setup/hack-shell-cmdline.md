# Shell Script Command Line & Environment Variable Reference

This document records the exact command line interface, environment variables, default values, side effects, and behavioral semantics of `hack/install-ate.sh`, `hack/install-ate-kind.sh`, and the sourced `hack/install-demo-*.sh` scripts. It serves as the golden baseline specification for testing `cmd/ate-setup` and the bridge compatibility stubs.

## 1. Global & Configuration Flags / Environment Variables

The shell installer pre-scanned these value-bearing flags so they could appear anywhere before or after action flags.

| Flag | Environment Variable | Default | Type | Description |
|---|---|---|---|---|
| `--kind` | `ATE_INSTALL_KIND` | `false` | Boolean | Selects Kind local cluster profile (manifest overlays in `manifests/ate-install/kind`, local registry `localhost:5001`, host-arch build). |
| `--atenet-router=X` or `--atenet-router X` | `ATE_ATENET_ROUTER` | `envoy` | Enum (`envoy`, `agentgateway`) | Selects the ingress and egress router dataplane. |
| `--rollout-timeout=X` or `--rollout-timeout X` | `ATE_INSTALL_ROLLOUT_TIMEOUT` | `60s` | Go Duration | Per-workload rollout status wait timeout for Deployments, DaemonSets, StatefulSets. |
| `--podcert-workers-per-signer=N` or `--podcert-workers-per-signer N` | `ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER` | `1` | Integer | Concurrent worker count per signer in podcertificate-controller. |
| `--experimental-use-sdsmint` | `ATE_EXPERIMENTAL_USE_SDSMINT` | `false` | Boolean | Deploys egress gateway with dynamic per-SNI certificate minting (MITM proxy mode). |
| `--experimental-additional-egress-extproc-service=SVC` | `ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE` | `""` | String (`NS/SVC:PORT`) | Configures an additional external processor filter on the decrypted egress path (requires Envoy and SDSMint). |
| `--otlp-endpoint=URL` or `--otlp-endpoint URL` | `ATE_OTLP_ENDPOINT` | `""` | URL String | Target URL for control plane and workload OTLP telemetry exporter. |
| `--context=NAME` | `KUBECTL_CONTEXT` | `""` | String | Target kubeconfig context. Defaults to Kind context `kind-${KIND_CLUSTER_NAME:-kind}` on Kind. |
| `--kubeconfig=PATH` | `KUBECONFIG` | `""` | Path | Explicit kubeconfig path. |
| `--no-dev-env` | `NO_DEV_ENV` | `""` | Boolean / Flag | Skip sourcing `.ate-dev-env.sh` at the repository root. |
| — | `BUCKET_NAME` | `ate-snapshots` (Kind) | String | GCS / RustFS snapshot bucket name. |
| — | `KO_DOCKER_REPO` | `gcr.io/${PROJECT_ID}/ate-images` | String | Container image registry prefix for `ko`. |
| — | `KO_DEFAULTPLATFORMS` | `linux/amd64` (or `linux/${goarch}`) | String | Target platform architecture for image builds. |
| — | `ANTHROPIC_API_KEY` | `""` | String | API key required for the `demo-claude-code-multiplex` workload build. |

## 2. Overall System Actions

| Flag | Description | Execution Order / Prerequisites |
|---|---|---|
| `--deploy-ate-system` | Deploys the complete Agent Substrate control plane. | 1. Ensure CRDs (`manifests/generated`)<br>2. Ensure `ate-system` namespace<br>3. Ensure JWT authority pool secret<br>4. Ensure Actor ID CA pool secret & root certs secret<br>5. Ensure Egress MITM CA pool secret (if SDSMint enabled)<br>6. Ensure PodCertificate Controller CAs<br>7. Deploy PodCertificate Controller & wait for identity bundles (`120s`)<br>8. Deploy CSI drivers (if `--setup-csi` / `SETUP_CSI=true` on Kind)<br>9. Deploy Store (PostgreSQL)<br>10. Deploy API server env vars & authentication config<br>11. Deploy API server & wait for rollout<br>12. Deploy atenet (router + egress) & wait for rollout<br>13. Deploy atelet DaemonSet & wait for rollout. |
| `--delete-ate-system` | Tears down the control plane. | Deletes workloads in reverse order: atelet, atenet, ate-api-server, store, podcertificate-controller, CRDs, secrets, and namespace. |
| `--delete-all` | Full teardown of all demos followed by the control plane. | 1. Deletes all actors and manifests for every registered demo.<br>2. Runs `delete_ate_system`. |
| `--setup-csi` | Standalone CSI driver setup. | Kind only. Installs hostpath and NFS CSI drivers, creates StorageClasses and CSIDriverConfigs, restarts atelet. Warns and continues on GKE. |

## 3. Component Deployment Actions

| Flag | Description |
|---|---|
| `--deploy-atelet` | Applies `manifests/ate-install/atelet.yaml` (or Kind overlay) and waits for DaemonSet rollout (`60s`). |
| `--deploy-ate-apiserver` | Ensures secrets and config, applies `ate-api-server.yaml` (or Kind overlay), and waits for Deployment rollout (`60s`). |
| `--deploy-atenet` | Ensures Egress MITM CA pool (if SDSMint enabled), renders and applies `atenet-router` and `atenet-egress` manifests, and waits for rollout. |
| `--delete-atenet` | Deletes atenet router and egress manifests and components. |

## 4. Secret & Config Creation Actions

| Flag | Output Resource | Description |
|---|---|---|
| `--create-jwt-authority-pool-secret` | Secret `jwt-authority-pool` (ns: `ate-system`) | Generates RSA 2048 keypair for JWT signing and minting. |
| `--create-actor-id-ca-pool-secret` | Secret `actor-id-ca-pool` (ns: `ate-system`) | Generates ECDSA P-256 CA pool for actor client identities. |
| `--create-actor-id-ca-certs-secret` | Secret `actor-id-ca-certs` (ns: `ate-system`) | Derives root certificate trust bundle from `actor-id-ca-pool`. |
| `--create-egress-mitm-ca-pool-secret` | Secret `egress-mitm-ca-pool` (ns: `ate-system`) | Generates ECDSA P-256 CA pool for dynamic egress TLS certificate minting. |
| `--create-podcertificate-controller-cas` | Secrets `servicedns-ca-pool`, `podidentity-ca-pool` (ns: `podcertificate-controller-system`) | Generates CA pools for Service DNS and Pod Identity. |
| `--create-api-server-env-vars` | ConfigMap `api-server-env-vars` (ns: `ate-system`) | Writes database connection string and authority key configurations. |
| `--create-api-authentication-config` | ConfigMap `api-authentication-config` (ns: `ate-system`) | Writes apiserver webhook authentication configuration. |

## 5. Benchmark Actions

| Flag | Default | Description |
|---|---|---|
| `--deploy-benchmarks` | — | Deploys benchmark worker pools, locust load test stack, and telemetry. |
| `--delete-benchmarks` | — | Deletes locust stack and benchmark workloads. |
| `--benchmark-worker-count=N` | `1` | Number of WorkerPool replicas for benchmark workloads. |
| `--benchmark-sandbox-class=CLASS` | `gvisor` | Sandbox runtime: `gvisor` or `microvm` (requires `install-microvm-deps.sh`). |
| `--benchmark-actor-memory=SIZE` | `256Mi` | Memory limit for benchmark ActorTemplates. |

## 6. Demos Reference Matrix

| Demo Name | Deploy Flag | Delete Flag | Specific Flags / Options | Rollout / Readiness Waits | Notes |
|---|---|---|---|---|---|
| `demo-counter` | `--deploy-demo-counter` | `--delete-demo-counter` | `--deploy-demo-counter-with-external-volume` | Deployment: `300s`<br>ActorTemplate: `300s` | CRD-based counter demo exercising snapshot, resume, and ingress. Supports CSI external volume validation. |
| `demo-counter-substrate` | `--deploy-demo-counter-substrate`<br>`--deploy-demo-counter-substrate-microvm` | `--delete-demo-counter-substrate`<br>`--delete-demo-counter-substrate-microvm` | — | WorkerPool: `300s`<br>Golden Snapshot: `300s` (gVisor) / `600s` (micro-VM) | Substrate-resource counter demo. Creates `atespace` and `ActorTemplate` via ate API. |
| `demo-egress` | `--deploy-demo-egress`<br>`--deploy-demo-egress-microvm`<br>`--deploy-demo-egress-mitm`<br>`--deploy-demo-egress-microvm-mitm` | `--delete-demo-egress`<br>`--delete-demo-egress-microvm`<br>`--delete-demo-egress-mitm`<br>`--delete-demo-egress-microvm-mitm` | — | Deployment: `300s`<br>ActorTemplate: `300s` (gVisor) / `600s` (micro-VM) | Tests egress policy enforcement. MITM variants require SDSMint install. Micro-VM variants require microvm dependencies. |
| `demo-jupyter` | `--deploy-demo-jupyter` | `--delete-demo-jupyter` | — | ActorTemplate: `300s` | Jupyter notebook server demo workload. |
| `demo-sandbox` | `--deploy-demo-sandbox` | `--delete-demo-sandbox` | — | Manifest apply | On-demand sandbox actor driven by sandbox client. |
| `demo-claude-code-multiplex` | `--deploy-demo-claude-code-multiplex` | `--delete-demo-claude-code-multiplex` | Requires `ANTHROPIC_API_KEY`, `BUCKET_NAME`, `KO_DOCKER_REPO` | Manifest apply | Claude Code agents multiplexed onto a single WorkerPool. Builds custom Docker workload image. |
| `demo-multi-template` | `--deploy-demo-multi-template` | `--delete-demo-multi-template` | — | Deployment: `300s`<br>ActorTemplates: `300s` (`counter` & `fspersist`) | Two ActorTemplates sharing a single WorkerPool. |
| `demo-parking` | `--deploy-demo-parking` | `--delete-demo-parking` | — | Deployment: `300s`<br>ActorTemplate: `300s` | Actor parking and unparking on a constrained WorkerPool. |
| `demo-autoscaled-workerpool` | `--deploy-demo-autoscaled-workerpool` | `--delete-demo-autoscaled-workerpool` | Kind only | Deployment: `300s`<br>ActorTemplate: `300s`<br>Prometheus Adapter: `120s` | WorkerPool scaled by HPA using custom Prometheus metrics. |

## 7. Execution Semantics & Edge Cases

1. **Multi-Action Command Line Execution**:
   - `hack/install-ate.sh` iterated over arguments and executed each action in sequence order (e.g. `hack/install-ate.sh --deploy-ate-system --deploy-demo-counter`).
   - Failure of any action immediately aborts execution (`set -e`).
2. **Environment File Sourcing**:
   - Automatically sources `.ate-dev-env.sh` unless `NO_DEV_ENV=1` or `ATE_INSTALL_KIND=true`.
3. **Cluster Credential Auto-Discovery**:
   - When `PROJECT_ID` is set and `KUBECTL_CONTEXT` is unset, runs `gcloud container clusters get-credentials "${CLUSTER_NAME}" --location "${CLUSTER_LOCATION}" --project="${PROJECT_ID}"`.
4. **Error Handling**:
   - Unknown options display error and full usage message, exiting with code 1.
   - Missing required values for value flags (e.g. `--atenet-router`) abort immediately with code 1.
