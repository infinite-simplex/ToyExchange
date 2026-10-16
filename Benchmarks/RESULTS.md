# Ingress transport comparison: hand-written vs Aeron

Question: does Aeron beat Core's hand-written ingress transport (gateway multicast, receiver,
store, retransmit server) on throughput, latency or reliability, enough to justify a port?

## Setup

- Machine: AMD Ryzen 7 5800X (8 cores, 16 threads), WSL2, kernel 6.18, loopback. Every process is on one host.
- Core: v0.2.1 plus the `NetworkGateway` fan-out seam added with these benchmarks.
- Aeron: 1.53.3, the **C media driver** (`aeronmd`) and the C++ client. Not the Java driver.
- Harness: `IngressBench` (see `README.md`). Open-loop client, 128-byte records, 1,400-byte
  datagram or message limit (10 records), 2 or 3 replicas, 1 s warm-up, 5 s window, 3 repeats, medians.
- Latency is scheduled send time to dequeue on the replica, in microseconds. `cpu` is CPU
  microseconds per command per replica, including busy-spinning and all media drivers.
- Every run below had 0 sequence violations unless it says otherwise.

Transports:

| Name | What it is |
|---|---|
| hand-written, block | Core as released: receivers block in `recv()`. |
| hand-written, spin | Core with `ingressSpinMicros = 100`. |
| Aeron IPC | Shared memory, one driver. One host only. |
| Aeron UDP | UDP multicast, **one driver per replica** (the separate-host topology). Default `fc=max` flow control. |
| spin / backoff | Aeron driver and receiver busy-spin, or use Aeron's backoff idle strategy. |

Two replicas that share one driver also share one received Aeron stream, and the slowest of
them holds the window back for both. That is not how separate hosts behave, so the Aeron UDP
numbers use one driver per replica.

## Throughput and latency

p50 / p99 / p99.9 in microseconds.

**500,000 commands/s**

| Transport | 2 replicas | cpu | 3 replicas | cpu |
|---|---|---|---|---|
| hand-written, block | cannot keep up (232k/s) | | gives up | |
| hand-written, spin | 38 / 201 / 409 | 6.0 | 48 / 261 / 489 | 5.3 |
| Aeron IPC, spin | 39 / 107 / 1,985 | 8.7 | 40 / 194 / 10,551 | 7.1 |
| Aeron IPC, backoff | 68 / 160 / 242 | 4.5 | 71 / 166 / 264 | 3.9 |
| Aeron UDP, spin | 63 / 403 / 7,385 | 8.6 | 73 / 648 / 5,579 | 7.7 |
| Aeron UDP, backoff | 152 / 449 / 4,571 | 6.2 | 149 / 674 / 7,111 | 5.7 |

**1,000,000 commands/s**

| Transport | 2 replicas | 3 replicas |
|---|---|---|
| hand-written, spin | 78 / 8,118 / 8,610 | 1,042 / 15,548 / 16,703 |
| Aeron IPC, spin | 50 / 122 / 996 | 52 / 141 / 6,107 |
| Aeron IPC, backoff | 76 / 211 / 4,387 | 80 / 242 / 6,881 |
| Aeron UDP, spin | 673 / 16,957 / 19,104 | overloaded (p50 66 ms) |

**Ceiling, 2 replicas** (commands/s delivered when asked for more)

| Target | hand-written, spin | Aeron IPC | Aeron UDP |
|---|---|---|---|
| 1.5M | 1.34M | 1.50M | 1.30M |
| 2M | 1.35M | 2.00M | 1.31M |
| 3M | 1.33M | 3.00M | 1.31M |

## Reliability

**A replica's consumer stalls for 1.5 s** (2 replicas, 500k/s). Does the healthy replica notice, and does the stalled one recover?

| Transport | Healthy replica | Stalled replica |
|---|---|---|
| hand-written, spin | p50 40 µs, p99 0.2 to 1.5 ms | recovers through retransmit, nothing missing |
| Aeron UDP, default `fc=max` | p50 64 µs, p99 0.4 ms | **never recovers**: about 1.4M commands missing, no error raised |
| Aeron UDP, `fc=min` | stalled with it: p99 1.45 s | lossless |
| Aeron IPC | stalled with it: p99 about 1 s | lossless |

**A replica joins late**, 2 s into a 5 s run at 300k/s.

| Transport | Result |
|---|---|
| hand-written | Replays from sequence 0 through the retransmit server. Nothing missing. The other replica is unaffected. |
| Aeron IPC, Aeron UDP | Starts at the live position (about sequence 900,000), sees a gap from 0, gives up. 1.8M commands missing. |

**Idle CPU of a media driver** (no traffic)

| Driver configuration | CPU |
|---|---|
| defaults (3 threads, backoff) | 4% of one core |
| 3 threads, spin | 301% |
| 1 shared thread, spin | 100% |
| 1 shared thread, backoff | 1% |

Hand-written receivers block when idle. With `ingressSpinMicros` they spin only for the set window after a datagram.

## What this shows

- Aeron UDP is **not faster** than the hand-written transport here. At 500k/s its median latency is higher (63 to 73 µs against 38 to 48 µs) and its CPU cost per command is higher at the same rate. Both top out near 1.3M/s on this machine.
- Aeron IPC is clearly faster (3M/s and above, low tails), but it only works on one host. Replicas on one host do not give independent failure.
- A bare Aeron stream does not replace the sequence store and retransmit server. A late or lagging replica cannot catch up, so replay would need the Aeron Archive (a Java process) or Core's own store.
- Aeron's flow control forces a choice between losing the slow replica's stream (`fc=max`) and stalling the sequencer for everyone (`fc=min`, IPC). The hand-written design avoids both.

## What this does not show

- One machine, WSL2, loopback, 3 repeats. Tails include hypervisor noise.
- The C driver, not the Java one. Default MTU (1,408). No core pinning. No kernel bypass.
- The Aeron rig is not tuned beyond the driver settings in `Benchmarks/` scripts: one `offer` per batch, no `tryClaim`.
- Multi-host behavior. A real network with real loss, a separate driver per host, jumbo frames and bare-metal latency could change the ranking.
- A hybrid (Aeron for the live path, Core's store and retransmit for catch-up) was not built or measured.
