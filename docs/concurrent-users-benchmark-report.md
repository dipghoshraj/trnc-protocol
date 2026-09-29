# Concurrent-user benchmark report

## Scope

This report records the reproducible concurrent-user workload supported by the benchmark harness. It is a loopback echo comparison, not a production capacity claim: all servers and clients run in one process on `127.0.0.1`, TLS is off, and every response payload is verified.

## Workload

| Setting | Value |
| --- | --- |
| Protocols | TRNC, gRPC (h2c), REST (HTTP/1.1) |
| Concurrent users | 10 independently connected clients |
| Warm-up | 100 requests per user, excluded from results |
| Measured requests | 10,000 total per protocol (1,000 per user) |
| Payload | 256 deterministic bytes |
| Metrics | Aggregate throughput and per-operation min/p50/p95/p99/max round-trip latency |

Run this exact workload and retain the machine-readable rows with the revision:

```bash
mkdir -p benchmarks/results
cargo run --release --manifest-path benchmarks/Cargo.toml -- \
  --protocol all --warmup 100 --requests 10000 --concurrent-users 10 --payload-bytes 256 \
  | tee "benchmarks/results/$(git rev-parse --short HEAD)-10-users.txt"
```

## Result format

The runner emits one `RESULT` line per protocol. It includes `concurrent_users=10`, the completed total operation count, elapsed time, aggregate operations per second, and the per-operation latency percentiles. Keep the raw output rather than committing a single host-specific number; CPU scheduling, frequency scaling, Rust/dependency versions, and the loopback stack materially affect it.

## Interpretation

Compare only runs that use the same command and host. A higher concurrent-user count measures both request execution and load-induced queueing; it should not be compared directly with the default one-user latency baseline. For each configuration, run several samples and compare percentile ranges as well as throughput.
