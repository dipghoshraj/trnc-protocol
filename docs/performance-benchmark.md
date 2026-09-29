# Performance benchmark methodology

The repository includes a runnable loopback comparison in [`../benchmarks`](../benchmarks) for the custom TRNC protocol, gRPC, and REST. It measures an echo operation only, so application business logic does not dominate the result.

## Protocol setup

| Implementation | Request model | Connection reuse | TLS |
| --- | --- | --- | --- |
| TRNC | Handshake, then an `Open`/`Data`/`Close` logical stream for each echo | One TCP connection | Disabled |
| gRPC | Unary protobuf `Echo` RPC | One clear-text HTTP/2 (`h2c`) channel | Disabled |
| REST | Binary `POST /echo` | One clear-text HTTP/1.1 client connection | Disabled |

Each request sends the same deterministic byte payload and the benchmark checks that the returned bytes match before counting it. Server startup, client connection establishment, and warm-up requests are excluded from measured latency and throughput. Operations are intentionally sequential (`concurrency = 1`), which makes the reported latency a direct round-trip measure and avoids conflating it with queueing latency.

## Run and retain a result

Run from the repository root. The `tee` command keeps the raw, machine-readable `RESULT` rows alongside the code revision that produced them.

```bash
mkdir -p benchmarks/results
cargo run --release --manifest-path benchmarks/Cargo.toml -- \
  --protocol all --warmup 1000 --requests 10000 --payload-bytes 1024 \
  | tee "benchmarks/results/$(git rev-parse --short HEAD)-loopback.txt"
```

For a quick smoke run, use a smaller sample:

```bash
cargo run --release --manifest-path benchmarks/Cargo.toml -- \
  --protocol all --warmup 10 --requests 100 --payload-bytes 256
```

The benchmark prints a configuration row and one `RESULT` row for every protocol. Its result fields are:

- `operations`: successful, response-validated measured round trips.
- `elapsed_ms`: total wall-clock duration for the measured operations.
- `ops_per_sec`: sequential throughput, calculated as `operations / elapsed_seconds`.
- `latency_us_min`, `latency_us_p50`, `latency_us_p95`, `latency_us_p99`, and `latency_us_max`: per-operation round-trip latency values from an HDR histogram.

## Interpreting results

The benchmark is a baseline, not a claim of production performance. Results depend on CPU frequency scaling, Rust and dependency versions, the OS scheduler, and the loopback TCP stack. For a meaningful comparison across commits, keep the exact command, host, and payload size fixed; run it several times; and compare the percentile ranges rather than a single run. The committed benchmark deliberately has no TLS mode because TLS is not currently enabled for the TRNC path and all three measurements must use the same security setting.
