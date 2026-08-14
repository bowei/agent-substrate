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

ATE_DEMOS+=(demo-netdebug) # register demo-netdebug

demo-netdebug_cmdline() {
  case "${1}" in
    --deploy-demo-netdebug) demo-netdebug_deploy ;;
    --delete-demo-netdebug) demo-netdebug_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

# Build the workload image, push to ${KO_DOCKER_REPO}, and echo the resolved
# digest-pinned reference (e.g. localhost:5001/netdebug-workload@sha256:...).
demo-netdebug_build_workload() {
  local repo="${KO_DOCKER_REPO}/netdebug-workload"
  # shellcheck disable=SC2155 # safe initialization
  local stage_tag="${repo}:build-$(date +%s)"
  local platform="${KO_DEFAULTPLATFORMS:-linux/amd64}"
  local builder_args=()
  if [[ "${KO_DOCKER_REPO}" == localhost:* ]]; then
    builder_args+=("--builder" "default")
  fi
  docker buildx build \
    "${builder_args[@]}" \
    --platform="${platform}" \
    --push \
    -t "${stage_tag}" \
    demos/netdebug/workload >&2
  local digest
  digest=$(docker buildx imagetools "${builder_args[@]}" inspect "${stage_tag}" --format '{{json .}}' \
             | jq -r '.manifest.digest')
  if [[ -z "${digest}" || "${digest}" == "null" ]]; then
    echo "Failed to resolve workload image digest from ${stage_tag}" >&2
    return 1
  fi
  echo "${repo}@${digest}"
}

demo-netdebug_deploy() {
  log_step "demo-netdebug_deploy"
  ensure_crds

  if [[ -z "${BUCKET_NAME:-}" ]]; then
    echo "BUCKET_NAME must be set" >&2
    return 1
  fi
  if [[ -z "${KO_DOCKER_REPO:-}" ]]; then
    echo "KO_DOCKER_REPO must be set" >&2
    return 1
  fi

  local workload_image
  workload_image=$(demo-netdebug_build_workload)
  if [[ -z "${workload_image}" ]]; then
    return 1
  fi
  log_step "  workload image: ${workload_image}"

  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      -e "s|\${WORKLOAD_IMAGE}|${workload_image}|g" \
      demos/netdebug/netdebug.yaml.tmpl \
    | run_ko apply -f -

  log_step "Waiting for netdebug demo to be ready..."
  run_kubectl rollout status deployment/netdebug -n ate-demo-netdebug --timeout=300s
  run_kubectl wait --for=condition=Ready actortemplate/netdebug -n ate-demo-netdebug --timeout=300s
}

demo-netdebug_delete() {
  log_step "demo-netdebug_delete"
  delete_demo_actors ate-demo-netdebug netdebug
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME:-placeholder}|g" \
      -e "s|\${WORKLOAD_IMAGE}|placeholder|g" \
      demos/netdebug/netdebug.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}
