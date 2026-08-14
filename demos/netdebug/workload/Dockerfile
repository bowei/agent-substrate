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

# Stage 1: Build httpcmd binary
FROM golang:1.24-trixie AS builder

RUN git clone https://github.com/haccht/httpcmd.git /src \
 && cd /src \
 && go build -o /go/bin/httpcmd .

# Stage 2: Debian runtime with network debugging tools
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    dnsutils \
    httpie \
    iperf3 \
    iproute2 \
    iputils-ping \
    mtr-tiny \
    netcat-openbsd \
    procps \
    socat \
    tcpdump \
    traceroute \
    wget \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /go/bin/httpcmd /usr/local/bin/httpcmd
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 80

ENTRYPOINT ["/entrypoint.sh"]
