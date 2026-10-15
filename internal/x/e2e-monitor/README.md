# e2e-monitor

`e2e-monitor` is a local, read-only dashboard for a cluster running Agent Substrate.
It shows:

- **One tab per component.** Every Deployment, DaemonSet, and StatefulSet in the system namespace (and in each `--extra-namespace`) gets a tab, so new components appear without code changes. Each tab lists pods, container states, restarts, and recent events, and streams live logs that you can filter by pod, container, level, and text. The `atenet-router` tab also shows the router's `/statusz`.
- **Workers.** Every WorkerPool and its worker pods (pods labeled `ate.dev/worker-pool`), with e2e-owned pools flagged. The tab also shows ateapi's view: the registered workers and which actors run on them.
- **E2E.** Progress of an e2e run, read from its `go test -json` stream. For each test it shows a **timeline** that merges the test's own output with every component log line that mentions a resource the test created (its namespaces, atespaces, and actors). A swimlane chart, one row per component, shows how the test moved through ateapi, atecontroller, atelet, the router, and the worker pods.

It is experimental (`internal/x/`) and has no compatibility guarantees.

## Usage

The dashboard uses your kubeconfig and serves on `http://127.0.0.1:8080` by default.

```shell
# Cluster view only.
go run ./internal/x/e2e-monitor serve

# Run e2e tests through the monitor. It adds -json before -args, echoes the
# usual output to the terminal, and keeps serving after the run until Ctrl-C.
go run ./internal/x/e2e-monitor run -- hack/run-e2e-kind.sh ./internal/e2e/suites/demo -run TestActorLifecycle

# Attach to a run started elsewhere. With E2E_JUNIT_FILE=/tmp/e2e.xml,
# hack/run-e2e.sh writes the event stream to /tmp/e2e.json.
go run ./internal/x/e2e-monitor serve --e2e-json=/tmp/e2e.json

# Look at a recorded run, such as a CI artifact. The cluster tabs still show
# the live cluster.
go run ./internal/x/e2e-monitor serve --e2e-json=artifacts/e2e-gvisor.json --replay
```

| Flag | Default | Meaning |
|---|---|---|
| `--kubeconfig`, `--context` | standard loading rules | Cluster to monitor |
| `--listen` | `127.0.0.1:8080` | Dashboard address |
| `--namespace` | `$ATE_NAMESPACE` or `ate-system` | Substrate's system namespace |
| `--extra-namespace` | `podcertificate-controller-system` | More namespaces whose workloads become tabs |
| `--resource-prefix` | `$E2E_RESOURCE_PREFIX` | Prefix an install puts on resource names |
| `--follow-workers` | `e2e` | Worker pods whose logs are followed all the time: `e2e` (pods of running tests), `all`, or `none` |
| `--substrate-api` | `true` | Poll ateapi (workers, actors) and the router's `/statusz` |
| `--tail-lines` | `500` | Existing lines loaded when a container starts being followed |
| `--buffer-lines` | `5000` | Lines kept in memory per container |
| `run --exit-when-done` | `false` | Exit when the command exits |

## How it works

```
internal/x/e2e-monitor/
  main.go               flags, wiring, serve / run
  internal/discovery    informers -> components, pools, e2e namespaces
  internal/logs         per-container log followers, ring buffers, JSON parsing, fan-out
  internal/e2efeed      test2json reader, test tree, tracked resources, log matcher
  internal/substrate    ateapi workers/actors and router /statusz poller
  internal/server       JSON API, server-sent events, embedded UI (web/)
```

- **Discovery** uses shared informers, so nothing is polled. Pods are attached to the workload whose selector matches them. Pods that no workload selects show up under `other-pods.<namespace>`. Descriptions and the `/statusz` port come from a small table of known components. A workload missing from that table still gets a tab.
- **Logs** come from `pods/log` with `follow=true`. Each container keeps a ring buffer, and a stream reconnects from its last timestamp after it drops. Every long-running container of every component is followed all the time. Worker pods are followed according to `--follow-workers`. Any pod you open in the UI is also followed for five minutes after you last viewed it. Lines from deleted pods stay readable for 30 minutes.
- **E2E correlation** depends on tracking markers in the test output. The shared helpers in `internal/e2e` call `e2e.Track`, which logs `E2E-MONITOR {"kind":...,"name":...}`. test2json attributes each marker to the test that wrote it. `CreateNamespace` also adds an `ate.dev/e2e-test` annotation to the namespace, so namespaces a failed run left behind can still be traced to their test. A test inherits the resources its parent tracked. A log line matches a test when it names one of the test's resources: in an `ate.*` attribute, an object path, a `namespace/pod` reference, or a request dump. Names shorter than 8 characters are matched only in key/value or path context.

If a test creates resources directly instead of through the shared helpers, it can call `e2e.Track` or `e2e.TrackActor` itself so the monitor can correlate its logs.

## Security

The dashboard reads from the cluster and never changes it. It binds to localhost by default. Logs can contain sensitive data, so do not expose `--listen` beyond your machine.
