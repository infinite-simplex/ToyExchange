# Disruptor Exchange

[![CI](https://github.com/infinite-simplex/ToyExchange/actions/workflows/ci.yml/badge.svg)](https://github.com/infinite-simplex/ToyExchange/actions/workflows/ci.yml)

A low-latency exchange pipeline built around an LMAX Disruptor-style
core — lock-free SPSC ring buffers, a replicated and leader-elected matching
tier with real failover, a sequenced ingress gateway with a durable log and
gap-fill retransmit. The pipeline is product-agnostic: `NetworkGateway` and
`MatchingService` are templated on the wire protocol and command type, so a
product plugs its own matching logic into the same ingress → replication →
egress pipeline rather than rebuilding it.

This is a portfolio project, not a production exchange.

## What's here

Just the reusable core (`Core/`) and its telemetry/aggregation tooling
(`PerformanceService/`) — no product logic. Products consume this repo as a
dependency rather than living inside it.

## Products built on it

- **[EventContractLOB](https://github.com/infinite-simplex/EventContractLOB)**
  — a NASDAQ OUCH/ITCH-speaking limit order book for binary-outcome event
  contracts. The reference implementation; start there to see the pipeline
  in use.
- **GPU compute exchange** — planned, not yet implemented. A frequent
  batch auction for GPU compute capacity, proving the pipeline generalizes
  beyond a continuous limit order book.

Design rationale and tradeoffs are written up separately outside this repo.

## Building and running

Linux only (uses `epoll`, `mmap`, multicast sockets, `CLOCK_TAI` directly —
on Windows, build under WSL). This repo alone only builds `Core` and
`PerformanceService`; there's no standalone executable here — see a product
repo (e.g. EventContractLOB) to run the exchange end to end.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

## Testing

```bash
ctest --test-dir build --output-on-failure
```

Core primitives only: replica arbitration, telemetry, performance
aggregation. A product repo carries its own tests for whatever it builds on
top (matching engine, wire protocol, ingress pipeline). CI runs this on
every push.

## License

[MIT](LICENSE).
