# Benchmarks

Tools for measuring Core. They are not part of the default build.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DTOYEXCHANGE_BENCHMARKS=ON
cmake --build build -j"$(nproc)" --target IngressBench
```

## IngressBench

End-to-end ingress benchmark.

```
TCP client --> NetworkGateway --> transport --> R receivers --> R drain threads
```

- **Client.** One thread sends 8-byte frames at a target rate (`--rate`, commands per second;
  `0` means as fast as TCP allows). Each frame carries the time it was **scheduled** to be
  sent, not the time the client got round to sending it. A stall anywhere downstream therefore
  shows up as latency. It is not hidden (coordinated omission).
- **Gateway.** The real `NetworkGateway`. It parses, sequences, appends to its store and
  fans out through the transport.
- **Receivers.** The transport's receiver for each replica, handing records to a queue through
  the same `PushToInboundQueue` the production receiver uses.
- **Drain threads.** One per replica, standing in for the matching thread. Each pops commands,
  checks that it sees every command exactly once and in order, and records
  scheduled-send-to-dequeue latency in an HdrHistogram.
- **Records.** 128 bytes each, like the reference product's, so a 1,400-byte datagram holds 10.

Each run has a warm-up (not measured), a measured window and a 1 s drain. After the drain it
counts what is missing. The process exits non-zero if any replica saw a violation, is missing
commands, or gave up. `cpu_us/cmd` is the CPU time of this process, plus the media drivers named
with `--driver-pid`, divided by commands delivered to all replicas. It includes busy-spinning.

```bash
build/Benchmarks/IngressBench --transport handwritten --replicas 2 --rate 500000 \
    --seconds 5 --repeats 3 --spin-us 100
```

Other scenarios:

- `--stall-replica N --stall-at S --stall-ms MS` pauses replica N's consumer, like a slow matching thread.
- `--late-start-replica N --late-start-ms MS` starts replica N's receiver after the stream is under way.

### Transports

| `--transport` | What it is |
|---|---|
| `handwritten` | Core's gateway multicast, `MulticastIngressReceiver`, `RetransmitServer`. |
| `aeron-ipc` | Aeron over shared memory (one host only). |
| `aeron-udp` | Aeron over UDP multicast. |

The Aeron transports are an experiment (`Benchmarks/AeronRig.hpp`). They need an Aeron checkout and
build (C driver and C++ client, built with CMake 3.30 or later):

```bash
cmake -S . -B build -DTOYEXCHANGE_BENCHMARKS=ON \
    -DTOYEXCHANGE_AERON_SRC=$HOME/aeron/src -DTOYEXCHANGE_AERON_BUILD=$HOME/aeron/build
```

The media driver (`aeronmd`) is a separate process. Pass its pid with `--driver-pid` so its CPU
time counts, and its directory with `--aeron-dir`. To model replicas on separate hosts, run one driver
for the gateway and one per replica, and pass `--aeron-replica-dirs`. With a shared driver, replicas
on one machine share one received stream, and the slowest one holds the window back for all of them.

## Reading the numbers

- All numbers so far come from one machine: WSL2 on a Ryzen 7 5800X (8 cores, 16 threads), loopback,
  every process co-located. Tail latencies on WSL2 include hypervisor scheduling noise.
- Three repeats per cell, medians reported. That shows the spread, but it is not a statistical study.
- A busy-spinning thread burns a core whether or not there is traffic. Compare `cpu_us/cmd`
  at the same rate only.
