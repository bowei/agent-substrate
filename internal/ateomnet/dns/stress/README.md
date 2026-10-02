# DNS relay stress harness

Stress tests for the sandbox DNS relay (`internal/ateomnet/dns`). They run
unprivileged on loopback against fake upstream resolvers.

The scenarios are **skipped by default**, so `go test ./...` and `make test`
don't run them. To run them:

```sh
# All scenarios, 10s each (about 3 minutes):
go test ./internal/ateomnet/dns/stress -stress -v

# One scenario, longer, with a JSON report:
go test ./internal/ateomnet/dns/stress -stress -v \
    -run 'TestStress/NoisyNeighbor' \
    -stress.duration=30s -stress.report=/tmp/dns-stress.json

# Include the mixed-fault soak:
go test ./internal/ateomnet/dns/stress -stress -v -run 'TestStress/Soak' \
    -stress.soak=10m -timeout=30m

# Steady-state benchmarks (no flag needed):
go test ./internal/ateomnet/dns/stress -run '^$' -bench . -benchtime=5s
```

| Flag | Default | Meaning |
|---|---|---|
| `-stress` | `false` | Run the stress scenarios. |
| `-stress.duration` | `10s` | Length of each scenario. |
| `-stress.scale` | `1` | Multiplier for load QPS, concurrency and the number of quiet sandboxes. |
| `-stress.soak` | `0` | Also run the `Soak` scenario for this long. |
| `-stress.report` | none | Write every report to this path as JSON. |
| `-stress.relaylogs` | `false` | Show the relay's own logs, which are hidden below ERROR by default. |

Run without `-race` for representative numbers.

Thresholds are **report-only**: a miss is printed as `FINDING` and doesn't fail
the test. These do fail the test: goroutine or file-descriptor leaks after
teardown, a relay that keeps serving after it is closed, and answers that
don't match their query.

See the package documentation (`go doc ./internal/ateomnet/dns/stress`) for how
to read the reports and how to write new scenarios.
