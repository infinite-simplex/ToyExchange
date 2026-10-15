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
- **[BatchGPU](https://github.com/infinite-simplex/BatchGPU)** — a frequent
  batch auction for GPU compute capacity, proving the pipeline generalizes
  beyond a continuous limit order book.

Design rationale and tradeoffs are written up in the
[Toy Exchange Core design write-up](https://infinite-simplex.github.io/InfiniteLabs/#ToyExchangeCore).

## What a product must provide

Every replica applies the same sequenced command stream to its own engine,
so a product's engine must be **deterministic**: the same commands, applied
in order from sequence 0, give the same state and the same events on every
replica. Core relies on this for replay (late join and restart) and for
failover without any hand-over of state. In practice:

- Nothing that affects state depends on hash-container order, pointer values
  or randomness.
- Nothing that affects state depends on a floating-point result that can
  differ between hosts.
- A clock reading may stamp an emitted event. It must never feed back into
  state.
- The command stream is the only input. Anything that must affect every
  replica (a timed cancel, a batch clear) enters through the gateway as a
  sequenced command and does not fire locally.
- A command that makes the engine throw must throw on every replica.

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
aggregation, the ingress recovery path (sequence store, retransmit server,
multicast receiver — late join, restart, duplicates, lost tails, damaged
retransmit replies), and the ingress gateway with a stand-in wire protocol
(sequencing, store-before-multicast, datagram batching, split and broken
frames). The tests use a stand-in command type. A product repo carries its
own tests for whatever it builds on top (matching engine, real wire
protocol). CI runs this on every push.

## License

[MIT](LICENSE).
