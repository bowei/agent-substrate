#!/usr/bin/env bash
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

# Backward-compatibility bridge delegating to cmd/ate-setup.
set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "${ROOT}"

usage() {
  cat << 'HELP'
Usage: hack/install-ate.sh [options]

Overall infrastructure (all infrastructure components):

  --deploy-ate-system                    Deploy core system (CRDs, atelet, apiserver)
  --setup-csi                            Setup CSI hostpath and NFS drivers (Kind only)
  --delete-ate-system                    Delete core system
  --delete-all                           Delete core system and all registered demos
  --atenet-router=envoy|agentgateway     Select the ingress and egress dataplane (default: envoy)
  --podcert-workers-per-signer N         Concurrent workers per podcertificate-controller signer (default: 1)
  --rollout-timeout DURATION             Per-workload readiness wait timeout, kubectl-style Go duration (default: 60s)
  --otlp-endpoint URL                    Send all control plane telemetry to URL, not to the cluster default (see benchmarking/telemetry/README.md)

Experiments:

  --experimental-use-sdsmint             Deploy the egress gateway with per-SNI certificate minting (experimental)
  --experimental-additional-egress-extproc-service NS/SVC:PORT
                                         Run an additional ext_proc authorization filter, served by that Service.
                                         Requires --experimental-use-sdsmint. (experimental)

Infrastructure components:

  --deploy-atelet                        Deploy atelet only
  --deploy-ate-apiserver                 Deploy ate-api-server only
  --deploy-atenet                        Deploy atenet only
  --delete-atenet                        Delete atenet only

To create individual resources used by ate-system (Note: These are
called automatically by --deploy-ate-system):

  --create-jwt-authority-pool-secret     Create JWT authority pool secret
  --create-actor-id-ca-pool-secret       Create actor ID CA pool secret
  --create-actor-id-ca-certs-secret      Create actor ID CA certs secret
  --create-egress-mitm-ca-pool-secret    Create egress MITM CA pool secret
  --create-podcertificate-controller-cas Create podcertificate controller CAs
  --create-api-server-env-vars           Create ate-api-server env vars
  --create-api-authentication-config     Create the default ate-api-server authentication config

Benchmarks (see benchmarking/README.md for details and customization):

  --deploy-benchmarks                    Deploy workloads + locust load test stack
  --delete-benchmarks                    Delete the locust stack and workloads
  --benchmark-worker-count N             Number of WorkerPool replicas (default: 1)
  --benchmark-sandbox-class CLASS        Sandbox runtime for the benchmark WorkerPool: gvisor | microvm (default: gvisor).
                                         microvm requires hack/install-microvm-deps.sh --install to have run.
  --benchmark-actor-memory SIZE          Memory limit for the benchmark ActorTemplates (default: 256Mi,
                                         the smallest size microvm admits)

Demos:

  --deploy-demo-counter                         Deploy demo-counter
  --deploy-demo-counter-with-external-volume    Deploy demo-counter with CSI volume validation
  --delete-demo-counter                         Delete demo-counter
  --deploy-demo-counter-substrate               Deploy demo-counter-substrate on gVisor
  --deploy-demo-counter-substrate-microvm       Deploy demo-counter-substrate on micro-VM workers (needs install-microvm-deps.sh)
  --delete-demo-counter-substrate               Delete demo-counter-substrate
  --delete-demo-counter-substrate-microvm       Delete the micro-VM variant
  --deploy-demo-egress                          Deploy demo-egress
  --delete-demo-egress                          Delete demo-egress
  --deploy-demo-egress-microvm                  Deploy demo-egress on micro-VM workers
  --delete-demo-egress-microvm                  Delete demo-egress-microvm
  --deploy-demo-egress-mitm                     Deploy demo-egress with MITM certs
  --delete-demo-egress-mitm                     Delete demo-egress-mitm
  --deploy-demo-egress-microvm-mitm             Deploy demo-egress on micro-VM workers with MITM certs
  --delete-demo-egress-microvm-mitm             Delete demo-egress-microvm-mitm
  --deploy-demo-jupyter                         Deploy demo-jupyter
  --delete-demo-jupyter                         Delete demo-jupyter
  --deploy-demo-sandbox                         Deploy demo-sandbox
  --delete-demo-sandbox                         Delete demo-sandbox
  --deploy-demo-claude-code-multiplex           Deploy demo-claude-code-multiplex
  --delete-demo-claude-code-multiplex           Delete demo-claude-code-multiplex
  --deploy-demo-multi-template                  Deploy demo-multi-template
  --delete-demo-multi-template                  Delete demo-multi-template
  --deploy-demo-parking                         Deploy demo-parking
  --delete-demo-parking                         Delete demo-parking
  --deploy-demo-autoscaled-workerpool           Deploy demo-autoscaled-workerpool
  --delete-demo-autoscaled-workerpool           Delete demo-autoscaled-workerpool
HELP
}

GLOBAL_FLAGS=()
ACTIONS=()

SETUP_CSI="false"
BENCHMARK_WORKER_COUNT=""
BENCHMARK_SANDBOX_CLASS=""
BENCHMARK_ACTOR_MEMORY=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --kind)
      GLOBAL_FLAGS+=("--kind")
      shift
      ;;
    --atenet-router=*)
      GLOBAL_FLAGS+=("--atenet-router=${1#*=}")
      shift
      ;;
    --atenet-router)
      if [[ $# -lt 2 ]]; then echo "error: --atenet-router requires an argument" >&2; exit 1; fi
      GLOBAL_FLAGS+=("--atenet-router=$2")
      shift 2
      ;;
    --rollout-timeout=*)
      GLOBAL_FLAGS+=("--rollout-timeout=${1#*=}")
      shift
      ;;
    --rollout-timeout)
      if [[ $# -lt 2 ]]; then echo "error: --rollout-timeout requires an argument" >&2; exit 1; fi
      GLOBAL_FLAGS+=("--rollout-timeout=$2")
      shift 2
      ;;
    --podcert-workers-per-signer=*)
      GLOBAL_FLAGS+=("--podcert-workers-per-signer=${1#*=}")
      shift
      ;;
    --podcert-workers-per-signer)
      if [[ $# -lt 2 ]]; then echo "error: --podcert-workers-per-signer requires an argument" >&2; exit 1; fi
      GLOBAL_FLAGS+=("--podcert-workers-per-signer=$2")
      shift 2
      ;;
    --experimental-use-sdsmint)
      GLOBAL_FLAGS+=("--experimental-use-sdsmint")
      shift
      ;;
    --experimental-additional-egress-extproc-service=*)
      GLOBAL_FLAGS+=("--experimental-additional-egress-extproc-service=${1#*=}")
      shift
      ;;
    --experimental-additional-egress-extproc-service)
      if [[ $# -lt 2 ]]; then echo "error: --experimental-additional-egress-extproc-service requires an argument" >&2; exit 1; fi
      GLOBAL_FLAGS+=("--experimental-additional-egress-extproc-service=$2")
      shift 2
      ;;
    --otlp-endpoint=*)
      GLOBAL_FLAGS+=("--otlp-endpoint=${1#*=}")
      shift
      ;;
    --otlp-endpoint)
      if [[ $# -lt 2 ]]; then echo "error: --otlp-endpoint requires an argument" >&2; exit 1; fi
      GLOBAL_FLAGS+=("--otlp-endpoint=$2")
      shift 2
      ;;
    --context=*)
      GLOBAL_FLAGS+=("--context=${1#*=}")
      shift
      ;;
    --context)
      if [[ $# -lt 2 ]]; then echo "error: --context requires an argument" >&2; exit 1; fi
      GLOBAL_FLAGS+=("--context=$2")
      shift 2
      ;;
    --kubeconfig=*)
      GLOBAL_FLAGS+=("--kubeconfig=${1#*=}")
      shift
      ;;
    --kubeconfig)
      if [[ $# -lt 2 ]]; then echo "error: --kubeconfig requires an argument" >&2; exit 1; fi
      GLOBAL_FLAGS+=("--kubeconfig=$2")
      shift 2
      ;;
    --no-dev-env)
      GLOBAL_FLAGS+=("--no-dev-env")
      shift
      ;;
    --setup-csi)
      SETUP_CSI="true"
      shift
      ;;
    --benchmark-worker-count=*)
      BENCHMARK_WORKER_COUNT="${1#*=}"
      shift
      ;;
    --benchmark-worker-count)
      if [[ $# -lt 2 ]]; then echo "error: --benchmark-worker-count requires an argument" >&2; exit 1; fi
      BENCHMARK_WORKER_COUNT="$2"
      shift 2
      ;;
    --benchmark-sandbox-class=*)
      BENCHMARK_SANDBOX_CLASS="${1#*=}"
      shift
      ;;
    --benchmark-sandbox-class)
      if [[ $# -lt 2 ]]; then echo "error: --benchmark-sandbox-class requires an argument" >&2; exit 1; fi
      BENCHMARK_SANDBOX_CLASS="$2"
      shift 2
      ;;
    --benchmark-actor-memory=*)
      BENCHMARK_ACTOR_MEMORY="${1#*=}"
      shift
      ;;
    --benchmark-actor-memory)
      if [[ $# -lt 2 ]]; then echo "error: --benchmark-actor-memory requires an argument" >&2; exit 1; fi
      BENCHMARK_ACTOR_MEMORY="$2"
      shift 2
      ;;
    --deploy-ate-system)
      ACTIONS+=("DEPLOY_ATE_SYSTEM")
      shift
      ;;
    --delete-ate-system)
      ACTIONS+=("delete ate-system")
      shift
      ;;
    --delete-all)
      ACTIONS+=("delete all")
      shift
      ;;
    --deploy-atelet)
      ACTIONS+=("deploy atelet")
      shift
      ;;
    --deploy-ate-apiserver)
      ACTIONS+=("deploy apiserver")
      shift
      ;;
    --deploy-atenet)
      ACTIONS+=("deploy atenet")
      shift
      ;;
    --delete-atenet)
      ACTIONS+=("delete atenet")
      shift
      ;;
    --create-jwt-authority-pool-secret)
      ACTIONS+=("create jwt-authority-pool")
      shift
      ;;
    --create-actor-id-ca-pool-secret)
      ACTIONS+=("create actor-id-ca-pool")
      shift
      ;;
    --create-actor-id-ca-certs-secret)
      ACTIONS+=("create actor-id-ca-certs")
      shift
      ;;
    --create-egress-mitm-ca-pool-secret)
      ACTIONS+=("create egress-mitm-ca-pool")
      shift
      ;;
    --create-podcertificate-controller-cas)
      ACTIONS+=("create podcertificate-controller-cas")
      shift
      ;;
    --create-api-server-env-vars)
      ACTIONS+=("create api-server-env-vars")
      shift
      ;;
    --create-api-authentication-config)
      ACTIONS+=("create api-authentication-config")
      shift
      ;;
    --deploy-benchmarks)
      ACTIONS+=("DEPLOY_BENCHMARKS")
      shift
      ;;
    --delete-benchmarks)
      ACTIONS+=("DELETE_BENCHMARKS")
      shift
      ;;
    --deploy-demo-counter)
      ACTIONS+=("deploy demo counter")
      shift
      ;;
    --deploy-demo-counter-with-external-volume)
      ACTIONS+=("deploy demo counter --with-external-volume")
      shift
      ;;
    --delete-demo-counter)
      ACTIONS+=("delete demo counter")
      shift
      ;;
    --deploy-demo-counter-substrate)
      ACTIONS+=("deploy demo counter-substrate")
      shift
      ;;
    --delete-demo-counter-substrate)
      ACTIONS+=("delete demo counter-substrate")
      shift
      ;;
    --deploy-demo-counter-substrate-microvm)
      ACTIONS+=("deploy demo counter-substrate-microvm")
      shift
      ;;
    --delete-demo-counter-substrate-microvm)
      ACTIONS+=("delete demo counter-substrate-microvm")
      shift
      ;;
    --deploy-demo-egress)
      ACTIONS+=("deploy demo egress")
      shift
      ;;
    --delete-demo-egress)
      ACTIONS+=("delete demo egress")
      shift
      ;;
    --deploy-demo-egress-microvm)
      ACTIONS+=("deploy demo egress-microvm")
      shift
      ;;
    --delete-demo-egress-microvm)
      ACTIONS+=("delete demo egress-microvm")
      shift
      ;;
    --deploy-demo-egress-mitm)
      ACTIONS+=("deploy demo egress-mitm")
      shift
      ;;
    --delete-demo-egress-mitm)
      ACTIONS+=("delete demo egress-mitm")
      shift
      ;;
    --deploy-demo-egress-microvm-mitm)
      ACTIONS+=("deploy demo egress-microvm-mitm")
      shift
      ;;
    --delete-demo-egress-microvm-mitm)
      ACTIONS+=("delete demo egress-microvm-mitm")
      shift
      ;;
    --deploy-demo-jupyter)
      ACTIONS+=("deploy demo jupyter")
      shift
      ;;
    --delete-demo-jupyter)
      ACTIONS+=("delete demo jupyter")
      shift
      ;;
    --deploy-demo-sandbox)
      ACTIONS+=("deploy demo sandbox")
      shift
      ;;
    --delete-demo-sandbox)
      ACTIONS+=("delete demo sandbox")
      shift
      ;;
    --deploy-demo-claude-code-multiplex)
      ACTIONS+=("deploy demo claude-code-multiplex")
      shift
      ;;
    --delete-demo-claude-code-multiplex)
      ACTIONS+=("delete demo claude-code-multiplex")
      shift
      ;;
    --deploy-demo-multi-template)
      ACTIONS+=("deploy demo multi-template")
      shift
      ;;
    --delete-demo-multi-template)
      ACTIONS+=("delete demo multi-template")
      shift
      ;;
    --deploy-demo-parking)
      ACTIONS+=("deploy demo parking")
      shift
      ;;
    --delete-demo-parking)
      ACTIONS+=("delete demo parking")
      shift
      ;;
    --deploy-demo-autoscaled-workerpool)
      ACTIONS+=("deploy demo autoscaled-workerpool")
      shift
      ;;
    --delete-demo-autoscaled-workerpool)
      ACTIONS+=("delete demo autoscaled-workerpool")
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${SETUP_CSI}" == "true" && ${#ACTIONS[@]} -eq 0 ]]; then
  ACTIONS+=("setup csi")
fi

if [[ ${#ACTIONS[@]} -eq 0 ]]; then
  usage >&2
  exit 1
fi

ATESETUP_CMD=()
if [[ -x "${ROOT}/bin/ate-setup" ]]; then
  ATESETUP_CMD=("${ROOT}/bin/ate-setup")
else
  ATESETUP_CMD=(go run ./cmd/ate-setup)
fi

for action in "${ACTIONS[@]}"; do
  CMD_ARGS=()
  case "${action}" in
    DEPLOY_ATE_SYSTEM)
      CMD_ARGS=("deploy" "ate-system")
      if [[ "${SETUP_CSI}" == "true" ]]; then
        CMD_ARGS+=("--setup-csi")
      fi
      ;;
    DEPLOY_BENCHMARKS)
      CMD_ARGS=("deploy" "benchmarks")
      if [[ -n "${BENCHMARK_WORKER_COUNT}" ]]; then
        CMD_ARGS+=("--worker-count=${BENCHMARK_WORKER_COUNT}")
      fi
      if [[ -n "${BENCHMARK_SANDBOX_CLASS}" ]]; then
        CMD_ARGS+=("--sandbox-class=${BENCHMARK_SANDBOX_CLASS}")
      fi
      if [[ -n "${BENCHMARK_ACTOR_MEMORY}" ]]; then
        CMD_ARGS+=("--actor-memory=${BENCHMARK_ACTOR_MEMORY}")
      fi
      ;;
    DELETE_BENCHMARKS)
      CMD_ARGS=("delete" "benchmarks")
      if [[ -n "${BENCHMARK_SANDBOX_CLASS}" ]]; then
        CMD_ARGS+=("--sandbox-class=${BENCHMARK_SANDBOX_CLASS}")
      fi
      ;;
    *)
      # Split action string on spaces into CMD_ARGS array
      read -r -a CMD_ARGS <<< "${action}"
      ;;
  esac

  "${ATESETUP_CMD[@]}" "${GLOBAL_FLAGS[@]}" "${CMD_ARGS[@]}"
done
