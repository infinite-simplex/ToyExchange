# Event Contract Exchange

[![CI](https://github.com/infinite-simplex/ToyExchange/actions/workflows/ci.yml/badge.svg)](https://github.com/infinite-simplex/ToyExchange/actions/workflows/ci.yml)

A low-latency matching engine and exchange stack for event contracts (binary
outcomes priced 0-100, in the style of Kalshi/CME event contracts), built
from scratch in C++20 and modeled directly on real exchange architecture:
the LMAX Disruptor pattern, NASDAQ's OUCH/ITCH wire protocols, and a
replicated, leader-elected matching tier with real failover.

This is a portfolio project. It is not a production exchange, and this
README says exactly where the line is.

## Architecture

Three independent processes, each its own executable, coordinating over UDP
multicast and TCP:

- **NetworkServiceApp** (`NetworkService/`) — the gateway. Accepts OUCH
  order-entry connections over TCP, sequences every command, durably logs it
  (an mmap-backed ring buffer, read concurrently via a seqlock so a lagging
  replica can catch up without blocking the writer), and multicasts it to
  every matching replica. Also runs leader arbitration and the egress path
  (private acks back to clients, public broadcast).
- **MatchingServiceApp** (`MatchingService/`) — the matching engine. One or
  more replicas, each independently consuming the identical sequenced
  command stream and maintaining its own copy of the order book, so any
  replica can take over instantly if another one dies. Only the replica
  currently elected leader actually publishes results — see Failover below.
- **PerformanceServiceApp** (`PerformanceService/`) — observability. Each
  replica forwards per-order timing telemetry here; it aggregates
  per-replica HdrHistogram percentiles (p50/p99/p99.99) across latency
  measured for `engine execution time`, `network delay`, and `queuing
  delay`, dumped to the console on a rolling window.

Order flow: a client sends a real NASDAQ OUCH 4.2 frame over TCP to
NetworkServiceApp → it is sequenced and multicast → every MatchingServiceApp
replica applies it to its own book via a lock-free SPSC ring buffer (the
LMAX Disruptor pattern) → the leader replica's resulting events are
multicast back → NetworkServiceApp turns them into a private ack for the
originating client and a public broadcast.

### Failover

Every replica sends a heartbeat carrying its own id and how far it has
gotten through the sequence; the gateway's arbiter replies with the current
leader and a fencing epoch, bumped on every leadership change. Only the
replica that believes itself leader actually publishes — so if it dies, a
surviving replica is promoted within a couple of heartbeat intervals, the
epoch bumps, and any stray traffic from the dead leader is dropped by epoch
comparison rather than trusted. `Tools/two_replica_smoke_test.sh` exercises
this end to end, including killing the leader mid-session and confirming
the survivor takes over.

### What is real, not simplified

- Inbound order entry decodes genuine NASDAQ OUCH 4.2 wire structs
  (`NetworkService/OUCH_ITCH/OUCH.hpp`), including its real fixed-point
  price convention and `timeInForce` semantics (`0` = Immediate-or-Cancel,
  `99998` = auto-cancel at market close) — not a simplified lookalike
  format.
  An `OuchTestClient` tool exists specifically to exercise this as a real,
  independent OUCH client.
- Lock-free single-producer/single-consumer ring buffers on every hot path
  (ingress → matching, matching → egress, matching → telemetry), the same
  pattern LMAX's Disruptor popularized for low-latency trading systems.
  `-march=native -O3` in the release build.
  `__rdtscp`/cache-line-aligned (`alignas(64)`) telemetry structs are used
  where per-order timing actually matters.
  A 128-bit bitset with hardware bit-scan (`__builtin_ctzll`/`clzll`) finds
  the best price level in O(1) rather than scanning.
- A real timing model for a distributed system: `CLOCK_TAI` (not
  `CLOCK_REALTIME`, which can step backward on a leap second) for any
  timestamp compared across machines, versus raw `rdtsc` cycles for
  same-machine deltas — see `ExchangeCommon/Telemetry.hpp`. `Tools/ptp/`
  documents a PTP setup for real clock synchronization across machines.
- Order types: `LIMIT`, `MARKET`, `IMMEDIATE_OR_CANCEL`, `FILL_OR_KILL`,
  `GOOD_TILL_CANCEL`, and `GOOD_TILL_DAY` (auto-cancels at 4pm EST via a
  dedicated scheduler service, leader-gated so only one replica ever fires
  it), plus `REPLACE`/`CANCEL`, self-trade prevention, and price-time
  priority matching.
- Gap recovery: a replica that misses multicast packets detects the gap
  from the sequence numbers and requests a retransmit from the gateway's
  durable log; if the gap is unrecoverable (evicted from the ring), the
  replica stops itself rather than trading on a silently incomplete book.

### What is simplified or not done

- Outbound acknowledgements and the public market-data broadcast use a
  compact custom frame, not real wire structs. `OUCH.hpp` actually defines
  the real outbound OUCH acknowledgement structs too (`OUCHOrderAccepted`,
  `OUCHOrderExecuted`, etc.) — they are just not wired into the egress path
  yet. There is no public ITCH feed with the full NASDAQ message set; an
  early, incomplete attempt at one was removed rather than left half-built.
  Both are natural next steps, not accidental gaps.
- The PTP cross-machine time-sync setup is documented (`Tools/ptp/`) but
  has not been verified end to end on real separate hardware.
- No pre-trade risk checks (position limits, fat-finger checks) exist.

## Building and running

Requires a Linux environment (this project uses POSIX APIs — `epoll`,
`mmap`, multicast sockets, `clock_gettime(CLOCK_TAI)` — directly; on
Windows, build and run it under WSL).

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

Run the full stack (one gateway, N matching replicas, one performance
collector) in the foreground:

```bash
./run_exchange.sh 2 build
```

Then, from another shell, send real OUCH orders:

```bash
build/Tools/OuchTestClient/OuchTestClient --side B --price 50 --qty 10 --firm-id 1
build/Tools/OuchTestClient/OuchTestClient --side S --price 50 --qty 10 --firm-id 2 --listen-ms 500
```

Watch live latency percentiles with `tail -f run/performance.log`, and test
failover for real with `./kill_leader.sh run` (kills only the current
leader replica, not the whole stack). `check_multicast.py` and
`Tools/two_replica_smoke_test.sh` are also available for diagnosing
multicast reachability and exercising the whole system end to end,
respectively.

## Testing

```bash
cmake --build build --target EngineTests -j"$(nproc)"
ctest --test-dir build --output-on-failure
```

87 tests across the matching engine, the full network ingress pipeline
(real OUCH frames over a real TCP socket into a real multicast receiver),
leader arbitration, telemetry, and the performance-aggregation pipeline. CI
runs this on every push (see `.github/workflows/ci.yml`).

## Project layout

```
ExchangeCommon/      Shared types (order/event structs, SPSC queue, aliases) —
                     header-only, depended on by every other module.
MatchingService/     The order book and matching engine.
NetworkService/      Ingress/, Egress/, Arbitration/, and OUCH_ITCH/ (wire protocol).
PerformanceService/  Telemetry aggregation and percentile reporting.
Tools/               OuchTestClient (a real OUCH client), PTP setup docs.
Testing.cpp          The full test suite (GoogleTest).
```

## License

[MIT](LICENSE).
