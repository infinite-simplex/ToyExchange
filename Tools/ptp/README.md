# PTP time sync for cross-machine telemetry

## Why this exists

`NetworkGateway` stamps `ingress_tai_ns` on its own host; `MatchingService`
replicas stamp `engine_pop_tai_ns`/`match_done_tai_ns` on theirs (see
`ExchangeCommon/Telemetry.hpp`). These values are only meaningful diffed
against each other if the two hosts' clocks actually agree — plain NTP gets
you roughly 1-10ms of accuracy, which is worse than the latencies this
system is trying to measure. Software PTP (`linuxptp`, no special NIC
required) realistically gets tens of microseconds on a LAN, which is enough
for these numbers to be meaningfully comparable instead of dominated by
clock error.

**Not verified end-to-end in this repo.** This is a standard `linuxptp`
deployment, not something invented for this project, but it hasn't been run
against a real second machine as part of building it — the current dev
setup is a single WSL VM with nothing to sync against. Treat this as a
documented starting point, and sanity-check it (see "Verifying it worked"
below) the first time it's actually deployed across real hosts.

## What to install

On every host (the gateway and every matching replica):

```bash
sudo apt-get install linuxptp
```

## Which config goes where

- **`ptp4l-master.conf`** + **`ptp4l-master.service`** → the `NetworkGateway`
  host only. Fixed as PTP master rather than relying on automatic Best
  Master Clock election — the topology here is small and already has an
  obvious always-up candidate (the same gateway this exchange already
  treats as its trusted arbiter elsewhere).
- **`ptp4l-slave.conf`** + **`ptp4l-slave.service`** → every `MatchingServiceApp`
  replica host. One copy each, nothing to change per-replica beyond the
  network interface name.

Setup, per host:

```bash
sudo mkdir -p /etc/linuxptp
sudo cp ptp4l-<master-or-slave>.conf /etc/linuxptp/
sudo cp ptp4l-<master-or-slave>.service /etc/systemd/system/
sudo sed -i 's/<interface>/eth0/' /etc/systemd/system/ptp4l-<master-or-slave>.service  # use the real NIC name
sudo systemctl daemon-reload
sudo systemctl enable --now ptp4l-<master-or-slave>
```

Both configs use `time_stamping software` (`-S` on the command line) —
this disciplines the kernel clock via `SO_TIMESTAMPING`/`adjtimex` using
ordinary UDP, no PTP-capable NIC or `phc2sys` needed. That's the deliberate
tradeoff described in the earlier design discussion: much simpler to deploy
than hardware timestamping, at the cost of tens-of-microseconds accuracy
instead of sub-microsecond.

## Why `CLOCK_TAI` specifically works here

`get_synced_time_ns()` (`Telemetry.hpp`) reads `clock_gettime(CLOCK_TAI, ...)`,
not `CLOCK_REALTIME` — TAI has no leap seconds, so it can never step
backward the way `CLOCK_REALTIME` occasionally does, even under a correctly
running sync daemon. This isn't a mismatch with using NTP/PTP: PTP's own
wire timescale *is* TAI by specification, and `ptp4l` applies the
`currentUtcOffset` it learns from the master's Announce messages to the
kernel's TAI offset as a normal part of the protocol — this is standard,
well-established `linuxptp` behavior, not something this project needs to
configure separately.

## Verifying it worked

Once both sides are running:

```bash
# On either host, ptp4l's own log output reports offset from master:
sudo journalctl -u ptp4l-slave -f
# Look for lines like "master offset   -142 s2 freq  +1234 ..." trending
# toward a small, stable offset (microseconds), not growing or oscillating.

# Or query directly via the management protocol:
sudo pmc -u -b 0 'GET CURRENT_DATA_SET'
```

Then, as an application-level sanity check: run `NetworkServiceApp` on the
master host and a `MatchingServiceApp` replica on a slave host, send an
order through, and confirm `engine_pop_tai_ns - ingress_tai_ns` (currently
only visible by inspecting `g_telemetry_arena` directly — there's no CLI
surface for it yet) comes out as a small positive number rather than
something wildly large or negative.
