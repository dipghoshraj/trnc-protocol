# Transport performance benchmarks

This package provides a repeatable **loopback echo** comparison of the TRNC transport, gRPC, and REST. It is intended to establish a local baseline, not to claim a universal ranking. Every implementation runs without TLS:

- **TRNC:** one TCP connection, TRNC handshake, and one logical stream per request.
- **gRPC:** unary protobuf echo over a persistent clear-text HTTP/2 (`h2c`) channel.
- **REST:** binary HTTP `POST /echo` requests over a reused clear-text HTTP/1.1 connection.

All servers and clients run in the same process and use `127.0.0.1`; the benchmark measures completed round trips from independently connected concurrent users. The payload is deterministic binary data and each response is verified before it is recorded. Warm-up operations are never included in the reported metrics.

## Run

Run all three protocols with the default 100 warm-up and 1,000 measured 256-byte operations:

```bash
cargo run --release --manifest-path benchmarks/Cargo.toml -- --protocol all
```

Use a larger sample and payload for a more stable local result:

```bash
cargo run --release --manifest-path benchmarks/Cargo.toml -- \
  --protocol all --warmup 1000 --requests 10000 --payload-bytes 1024
```

Run a single protocol while iterating:

```bash
cargo run --release --manifest-path benchmarks/Cargo.toml -- \
  --protocol trnc --warmup 10 --requests 100 --concurrent-users 10 --payload-bytes 256
```

The output has one machine-readable `RESULT` row per implementation, for example:

```text
RESULT protocol=Trnc tls=off concurrent_users=10 payload_bytes=256 operations=10000 elapsed_ms=... ops_per_sec=... latency_us_min=... latency_us_p50=... latency_us_p95=... latency_us_p99=... latency_us_max=...
```

## Metric definitions

| Metric | Definition |
| --- | --- |
| `concurrent_users` | Independently connected clients active concurrently. |
| `operations` | Successful, response-validated round trips measured across all users. |
| `elapsed_ms` | Wall-clock duration from the first measured request through the final measured response. |
| `ops_per_sec` | Aggregate throughput: `operations / elapsed_seconds`. |
| `latency_us_*` | Per-request wall-clock round-trip latency. `p50`, `p95`, and `p99` are HDR histogram percentiles. |

## Fairness and limits

The benchmark intentionally excludes connection setup from its measurements by establishing one reusable client connection/channel per concurrent user first. It includes each protocol's normal request framing, serialization, parsing, and response validation. TRNC creates and closes a logical stream per operation, matching the current `ResilientClient` request lifecycle. `--requests` is the total measured operation count and is divided as evenly as possible among `--concurrent-users`; warm-up is performed once per user.

These results are affected by CPU frequency, Rust version, dependency versions, OS scheduling, and local network stack. For comparisons between commits, keep the host, command, and payload unchanged; run several times and compare percentile ranges rather than a single number. This harness has no TLS mode so that all three protocols remain comparable to the current non-TLS TRNC path.
