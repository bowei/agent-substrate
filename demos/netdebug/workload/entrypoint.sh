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

set -o errexit -o nounset -o pipefail

ACTOR_ID="unknown"
if [[ -f /run/ate/actor-id ]]; then
  ACTOR_ID="$(cat /run/ate/actor-id)"
fi

echo "[netdebug-actor:${ACTOR_ID}] Starting httpcmd on port 80..."
exec /usr/local/bin/httpcmd --addr ":80" -x --permit-argument bash -c
