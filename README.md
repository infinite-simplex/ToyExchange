# Disruptor Exchange

[![CI](https://github.com/infinite-simplex/ToyExchange/actions/workflows/ci.yml/badge.svg)](https://github.com/infinite-simplex/ToyExchange/actions/workflows/ci.yml)

A low-latency exchange engine built around an LMAX Disruptor-style
pipeline — lock-free SPSC ring buffers, a replicated and leader-elected
matching tier with real failover, NASDAQ OUCH/ITCH wire protocols. The
pipeline itself is product-agnostic: `NetworkGateway`/`MatchingService` are
templated on the wire protocol and command type, so a new product plugs its
own matching logic into the same ingress → replication → egress pipeline
rather than rebuilding it.

This is a portfolio project, not a production exchange.

## Order books

- **Event contract exchange** — implemented. Binary-outcome contracts
  priced 0-100. The first product built on the pipeline.
- **GPU compute exchange** — planned, not yet implemented. Trading GPU
  compute capacity as the second product, proving the pipeline generalizes
  beyond one order book.

## Architecture

Three processes: **NetworkServiceApp** (gateway — OUCH ingress, sequencing,
durable log with gap-fill retransmit, leader arbitration, egress),
**MatchingServiceApp** (N replicas of the matching engine, only the elected
leader publishes), **PerformanceServiceApp** (HdrHistogram latency
percentiles aggregated across replicas). Real NASDAQ OUCH 4.2 wire format
on ingress; a custom compact frame on egress (no public ITCH feed yet).

Design rationale and tradeoffs are written up separately outside this repo.

## Building and running

Linux only (uses `epoll`, `mmap`, multicast sockets, `CLOCK_TAI` directly —
on Windows, build under WSL).

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
./run_exchange.sh 2 build
```

Then, from another shell:

```bash
build/Tools/OuchTestClient/OuchTestClient --side B --price 50 --qty 10 --firm-id 1
build/Tools/OuchTestClient/OuchTestClient --side S --price 50 --qty 10 --firm-id 2 --listen-ms 500
```

`./kill_leader.sh run` kills only the current leader replica, to watch
failover happen live. `tail -f run/performance.log` shows latency
percentiles.

## Testing

```bash
ctest --test-dir build --output-on-failure
```

87 tests: matching engine, full network ingress pipeline, leader
arbitration, telemetry, performance aggregation. CI runs this on every push.

## License

[MIT](LICENSE).
