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

// Package stress is a stress-test harness for the sandbox DNS relay,
// [dns.Relay].
//
// It runs the relay unprivileged on loopback: fake upstream resolvers
// ([upstream]) stand in for the worker pod's nameservers, and each simulated
// sandbox is served by [dns.Relay.ServeOn] on its own loopback sockets. All
// sandboxes share one relay, and so its limits, as they do on a worker. The
// network namespace bind that [dns.Relay.Serve] adds is not exercised.
//
// # Running the scenarios
//
// The scenarios in stress_test.go are skipped by default, so "go test ./..."
// and "make test" do not run them. Pass -stress to run them:
//
//	go test ./internal/ateomnet/dns/stress -stress -v
//
// Without -stress, only the harness's own quick self-tests (harness_test.go,
// about a second) run, so the harness keeps building and working in CI.
//
// Run one scenario, for longer, and save the reports as JSON:
//
//	go test ./internal/ateomnet/dns/stress -stress -v \
//	    -run 'TestStress/NoisyNeighbor' \
//	    -stress.duration=30s -stress.report=/tmp/dns-stress.json
//
// Flags (all after the package path):
//
//	-stress               run the stress scenarios (default false)
//	-stress.duration=D    length of each scenario (default 10s)
//	-stress.scale=F       multiply every load's QPS, concurrency and the
//	                      number of quiet sandboxes by F (default 1)
//	-stress.soak=D        also run the mixed-fault Soak scenario for D
//	                      (default 0: skipped)
//	-stress.report=PATH   write every scenario's report to PATH as JSON
//	-stress.relaylogs     show the relay's own logs; they are hidden below
//	                      ERROR by default because the relay logs a WARN per
//	                      failed query, which floods the output under load
//
// The whole suite takes roughly 15 × -stress.duration, plus up to a client
// timeout per scenario to collect the last answers. Raise -timeout (default
// 10m) for long durations or a soak, e.g. -timeout=1h.
//
// Each scenario logs a report like this one:
//
//	=== SlowUpstream: 4s, 1 sandbox(es)
//	  load  proto  sent  answered    ok%  timeout  skipped  malformed  qps       p50       p90       p99       max
//	  slow    udp  2000      1268  63.40      732        0          0  317  212.99ms  245.76ms  260.96ms  260.96ms
//	  slow: rcodes NOERROR=1268
//	  upstream 0: queries 1268, answered 1268, dropped 0, tcp conns 0, peak in flight 64
//	  resources: goroutines 5/925/5, fds 8/861/8, heap 2.5MiB/81.8MiB/3.1MiB (baseline/peak/after)
//	  FINDING (report-only): slow: answered 63.40%, want >= 99.00%
//
// Columns: "timeout" is queries the client gave up on (the relay drops UDP
// queries silently once its 64 slots are taken, so drops show up here);
// "skipped" is open-loop sends the client itself could not issue;
// "malformed" is answers that failed validation; "qps" is answers per second.
// Latencies are accurate to one histogram bucket (12.5%). "peak in flight"
// on an upstream is the most queries it held at once while delaying them.
//
// The clients and fake upstreams run in the same process as the relay, so
// the goroutine, descriptor and heap figures include theirs: compare them
// across runs and relay changes rather than reading them as absolutes.
//
// # Pass/fail policy
//
// Thresholds in a [scenario] are report-only: a miss is printed as a FINDING
// and does not fail the test, until baselines justify asserting them.
// Correctness failures do fail: goroutines or file descriptors not returning
// to their baseline after teardown, a relay that keeps serving after its
// sandbox is closed, and answers that do not match their query.
//
// Run without -race for representative numbers; with -race the report says so.
//
// # Benchmarks
//
// bench_test.go has steady-state benchmarks that need no flag:
//
//	go test ./internal/ateomnet/dns/stress -run '^$' -bench . -benchtime=5s
//
// # Writing a scenario
//
// A [scenario] lists upstream [behavior]s (in the order the relay tries
// them), a number of sandboxes, one [load] per simulated actor, and optional
// [phase]s that change an upstream's behavior partway through. [run] serves
// it and returns a [report]. For setups a scenario cannot express, such as
// sandboxes that come and go, use [newHarness], [harness.addSandbox],
// [sandbox.runLoad] and [sandbox.close] directly.
package stress
