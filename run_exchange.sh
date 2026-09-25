#!/usr/bin/env bash
# Launches the full replicated exchange stack for interactive use: one
# NetworkServiceApp gateway, N MatchingServiceApp replicas (which
# leader-elect among themselves — see NetworkService/Arbitration/), and one
# PerformanceServiceApp collecting telemetry forwarded from every replica.
# Stays in the foreground until interrupted (Ctrl+C), then cleanly stops
# every process it started.
#
# Usage:
#   ./run_exchange.sh <num-replicas> [build-dir]
#
# Run from a shell with the built binaries available (this repo builds under
# WSL/Linux — POSIX sockets/epoll — so run this under WSL too).
set -uo pipefail

if [ $# -lt 1 ] || ! [[ "$1" =~ ^[0-9]+$ ]] || [ "$1" -lt 1 ]; then
    echo "Usage: $0 <num-replicas> [build-dir]" >&2
    echo "  num-replicas must be a positive integer" >&2
    exit 1
fi
NUM_REPLICAS="$1"
BUILD_DIR="${2:-build}"

NET_BIN="$BUILD_DIR/NetworkService/NetworkServiceApp"
MATCH_BIN="$BUILD_DIR/MatchingService/MatchingServiceApp"
PERF_BIN="$BUILD_DIR/PerformanceService/PerformanceServiceApp"
CLIENT_BIN="$BUILD_DIR/Tools/OuchTestClient/OuchTestClient"

for bin in "$NET_BIN" "$MATCH_BIN" "$PERF_BIN"; do
    if [ ! -x "$bin" ]; then
        echo "Missing binary: $bin (build NetworkServiceApp, MatchingServiceApp, PerformanceServiceApp first)" >&2
        exit 1
    fi
done

RUN_DIR="run"
rm -rf "$RUN_DIR"
mkdir -p "$RUN_DIR"
SEQ_STORE="$RUN_DIR/sequence_store.dat"
NET_LOG="$RUN_DIR/network.log"
PERF_LOG="$RUN_DIR/performance.log"

PIDS=()
cleanup() {
    echo
    echo "Stopping..."
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" >/dev/null 2>&1 || true
    done
    for pid in "${PIDS[@]:-}"; do
        wait "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

"$NET_BIN" \
    --sequence-store "$SEQ_STORE" \
    --symbol TEST \
    > "$NET_LOG" 2>&1 &
PIDS+=("$!")

# Give the gateway a moment to bind its sockets before replicas try to join.
sleep 0.3

for i in $(seq 1 "$NUM_REPLICAS"); do
    "$MATCH_BIN" --id "replica-$i" --replica-id "$i" --stats-interval-ms 1000 \
        > "$RUN_DIR/replica-$i.log" 2>&1 &
    PIDS+=("$!")
done

"$PERF_BIN" --window-ms 1000 > "$PERF_LOG" 2>&1 &
PIDS+=("$!")

# Let heartbeats establish an initial leader and confirm nothing crashed
# immediately (bad port, missing binary, etc.) before printing "ready".
sleep 1.0

for pid in "${PIDS[@]}"; do
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "FAIL: a process exited immediately — check the logs in $RUN_DIR/" >&2
        cat "$RUN_DIR"/*.log >&2
        exit 1
    fi
done

echo "Exchange running with $NUM_REPLICAS replica(s):"
echo "  NetworkServiceApp   -> $NET_LOG   (OUCH order entry on :10001)"
for i in $(seq 1 "$NUM_REPLICAS"); do
    echo "  replica-$i            -> $RUN_DIR/replica-$i.log"
done
echo "  PerformanceService  -> $PERF_LOG   (percentiles dumped every 1s)"
echo
echo "Send orders with, e.g.:"
echo "  $CLIENT_BIN --side B --price 50 --qty 10 --firm-id 1"
echo
echo "Watch metrics live with:  tail -f $PERF_LOG"
echo "Test failover with:       ./kill_leader.sh $RUN_DIR   (kills just the current leader)"
echo "Press Ctrl+C to stop everything."

wait
