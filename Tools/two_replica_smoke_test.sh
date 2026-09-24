#!/usr/bin/env bash
# End-to-end smoke test: one NetworkServiceApp gateway + two MatchingServiceApp
# replicas consuming the same replication multicast feed, driven by a real
# OUCH client (OuchTestClient). Confirms both replicas converge to the same
# best bid/ask after a small resting + crossing order sequence, that only
# ONE replica's egress reaches the gateway per event (leader election —
# see NetworkService/Arbitration/), and that leadership fails over to the
# survivor when the current leader is killed.
#
# Run from a shell with the built binaries available (this repo is built
# under WSL/Linux — POSIX sockets/epoll — so run this under WSL too):
#   Tools/two_replica_smoke_test.sh [path-to-build-dir]
set -uo pipefail

BUILD_DIR="${1:-build}"
NET_BIN="$BUILD_DIR/NetworkService/NetworkServiceApp"
MATCH_BIN="$BUILD_DIR/MatchingService/MatchingServiceApp"
CLIENT_BIN="$BUILD_DIR/Tools/OuchTestClient/OuchTestClient"

for bin in "$NET_BIN" "$MATCH_BIN" "$CLIENT_BIN"; do
    if [ ! -x "$bin" ]; then
        echo "Missing binary: $bin (build NetworkServiceApp, MatchingServiceApp, OuchTestClient first)" >&2
        exit 1
    fi
done

MCAST_IP="239.10.10.10"
MCAST_PORT=31001
RETRANSMIT_PORT=41001
OUCH_PORT=11001
EGRESS_IP="239.10.10.11"
EGRESS_PORT=31501
ITCH_IP="239.10.10.12"
ITCH_PORT=21001
ARBITRATION_PORT=41501

WORKDIR="$(mktemp -d)"
SEQ_STORE="$WORKDIR/sequence_store.dat"
NET_LOG="$WORKDIR/network.log"
REPLICA_A_LOG="$WORKDIR/replica_a.log"
REPLICA_B_LOG="$WORKDIR/replica_b.log"
ITCH_LOG="$WORKDIR/itch_sniff.log"

PIDS=()
cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" >/dev/null 2>&1 || true
    done
    for pid in "${PIDS[@]:-}"; do
        wait "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

echo "Workdir: $WORKDIR"

# Independent sniffer on the public ITCH group, to prove a broadcast actually
# reaches a subscriber that isn't NetworkServiceApp itself. 5s timeout.
python3 - "$ITCH_IP" "$ITCH_PORT" > "$ITCH_LOG" 2>&1 <<'PYEOF' &
import socket, struct, sys
group, port = sys.argv[1], int(sys.argv[2])
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("", port))
mreq = struct.pack("4sl", socket.inet_aton(group), socket.INADDR_ANY)
s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
s.settimeout(5)
try:
    while True:
        data, _ = s.recvfrom(1024)
        print(f"ITCH packet: {data.hex()} len={len(data)}")
except socket.timeout:
    print("done listening")
PYEOF
PIDS+=("$!")

"$NET_BIN" \
    --ouch-port "$OUCH_PORT" \
    --multicast-ip "$MCAST_IP" --multicast-port "$MCAST_PORT" \
    --retransmit-port "$RETRANSMIT_PORT" \
    --sequence-store "$SEQ_STORE" \
    --symbol TEST \
    --egress-ip "$EGRESS_IP" --egress-port "$EGRESS_PORT" \
    --itch-ip "$ITCH_IP" --itch-port "$ITCH_PORT" \
    --arbitration-port "$ARBITRATION_PORT" \
    > "$NET_LOG" 2>&1 &
PIDS+=("$!")
NET_PID=$!

sleep 0.3

"$MATCH_BIN" --id replica-A --replica-id 1 --stats-interval-ms 300 \
    --multicast-ip "$MCAST_IP" --multicast-port "$MCAST_PORT" \
    --retransmit-ip 127.0.0.1 --retransmit-port "$RETRANSMIT_PORT" \
    --egress-ip "$EGRESS_IP" --egress-port "$EGRESS_PORT" \
    --arbitration-port "$ARBITRATION_PORT" \
    > "$REPLICA_A_LOG" 2>&1 &
PIDS+=("$!")
REPLICA_A_PID=$!

"$MATCH_BIN" --id replica-B --replica-id 2 --stats-interval-ms 300 \
    --multicast-ip "$MCAST_IP" --multicast-port "$MCAST_PORT" \
    --retransmit-ip 127.0.0.1 --retransmit-port "$RETRANSMIT_PORT" \
    --egress-ip "$EGRESS_IP" --egress-port "$EGRESS_PORT" \
    --arbitration-port "$ARBITRATION_PORT" \
    > "$REPLICA_B_LOG" 2>&1 &
PIDS+=("$!")
REPLICA_B_PID=$!

# Give heartbeats a moment to establish an initial leader (heartbeat
# interval is ~2ms, but leave real margin for process/scheduler jitter).
sleep 1.0

echo "Sending orders..."
# Price is a 0-100 scale in this engine (event-contract style pricing;
# OrderBook's price levels array is sized [0,100] — see MAX_LEVELS in
# MatchingService/OrderBook.hpp), so keep test prices in that range.
#
# Firm 1 rests a bid and firm 2 rests a (non-crossing) ask; firm 2's third
# order then crosses firm 1's resting bid. Distinct --firm-id values are
# required here — self-trade prevention would otherwise reject any order
# that crosses one from the same firm (see OuchOrderCommand::firmId).
"$CLIENT_BIN" --host 127.0.0.1 --port "$OUCH_PORT" --symbol TEST --side B --price 40 --qty 50 --firm-id 1
"$CLIENT_BIN" --host 127.0.0.1 --port "$OUCH_PORT" --symbol TEST --side S --price 60 --qty 50 --firm-id 2
"$CLIENT_BIN" --host 127.0.0.1 --port "$OUCH_PORT" --symbol TEST --side S --price 40 --qty 30 --firm-id 2 --listen-ms 800

sleep 1.0

STATE_A="$(grep 'best bid=' "$REPLICA_A_LOG" | tail -1)"
STATE_B="$(grep 'best bid=' "$REPLICA_B_LOG" | tail -1)"

echo "--- NetworkServiceApp log ---"
cat "$NET_LOG"
echo "--- replica-A log ---"
cat "$REPLICA_A_LOG"
echo "--- replica-B log ---"
cat "$REPLICA_B_LOG"

echo
echo "replica-A final: $STATE_A"
echo "replica-B final: $STATE_B"

if [ -z "$STATE_A" ] || [ -z "$STATE_B" ]; then
    echo "FAIL: one or both replicas never printed a stats line"
    exit 1
fi

# Book state (best bid/ask) must match between replicas; leader=/epoch=
# will legitimately differ (only one of them is ever leader), so strip
# those off before comparing.
BOOK_A="$(echo "$STATE_A" | sed -E 's/ leader=.*//')"
BOOK_B="$(echo "$STATE_B" | sed -E 's/ leader=.*//')"
NORM_A="${BOOK_A#*] }"
NORM_B="${BOOK_B#*] }"

if [ "$NORM_A" != "$NORM_B" ]; then
    echo "FAIL: replicas diverged on book state"
    exit 1
fi
echo "Book state converged -> $NORM_A"


# 3 orders sent, but the crossing 3rd order produces two EXECUTED events
# (one per leg of the trade) in addition to the first two orders' ACCEPTED
# events — 4 total, from a single leader. If leader election weren't
# working, every replica would publish independently and this would be a
# multiple of 4 (8 for two replicas) instead.
EGRESS_COUNT_1=$(grep -c '^\[egress\]' "$NET_LOG")
if [ "$EGRESS_COUNT_1" -ne 4 ]; then
    echo "FAIL: expected exactly 4 egress lines (2 ACCEPTED + 2 EXECUTED legs, from a single leader), got $EGRESS_COUNT_1"
    exit 1
fi
echo "PASS: exactly one replica published egress ($EGRESS_COUNT_1 lines for 3 orders / 1 trade)"

# --- Failover: kill whichever replica is currently leader, confirm the
# survivor takes over (with a strictly higher fencing epoch) and its output
# still reaches the gateway. ---
if grep -q 'leader=yes' "$REPLICA_A_LOG"; then
    LEADER_PID=$REPLICA_A_PID; LEADER_NAME="replica-A"
    LEADER_LOG="$REPLICA_A_LOG"
    SURVIVOR_LOG="$REPLICA_B_LOG"; SURVIVOR_NAME="replica-B"
elif grep -q 'leader=yes' "$REPLICA_B_LOG"; then
    LEADER_PID=$REPLICA_B_PID; LEADER_NAME="replica-B"
    LEADER_LOG="$REPLICA_B_LOG"
    SURVIVOR_LOG="$REPLICA_A_LOG"; SURVIVOR_NAME="replica-A"
else
    echo "FAIL: neither replica ever reported leader=yes"
    exit 1
fi

EPOCH_BEFORE=$(grep 'leader=yes' "$LEADER_LOG" | tail -1 | sed -E 's/.*epoch=([0-9]+).*/\1/')

echo "Killing current leader ($LEADER_NAME, epoch=$EPOCH_BEFORE) to test failover..."
kill "$LEADER_PID" 2>/dev/null
wait "$LEADER_PID" 2>/dev/null

# staleAfter is 10ms; leave real margin for scheduler/process jitter.
sleep 1.0

if ! grep -q 'leader=yes' "$SURVIVOR_LOG"; then
    echo "FAIL: $SURVIVOR_NAME never took over leadership after $LEADER_NAME was killed"
    echo "--- $SURVIVOR_NAME log ---"
    cat "$SURVIVOR_LOG"
    exit 1
fi

EPOCH_AFTER=$(grep 'leader=yes' "$SURVIVOR_LOG" | tail -1 | sed -E 's/.*epoch=([0-9]+).*/\1/')
if [ "$EPOCH_AFTER" -le "$EPOCH_BEFORE" ]; then
    echo "FAIL: fencing epoch did not increase on failover (before=$EPOCH_BEFORE, after=$EPOCH_AFTER)"
    exit 1
fi
echo "PASS: $SURVIVOR_NAME took over leadership (epoch $EPOCH_BEFORE -> $EPOCH_AFTER)"

echo "Sending one more order to confirm the survivor's output still reaches the gateway..."
"$CLIENT_BIN" --host 127.0.0.1 --port "$OUCH_PORT" --symbol TEST --side B --price 45 --qty 10 --firm-id 3

sleep 1.0

EGRESS_COUNT_2=$(grep -c '^\[egress\]' "$NET_LOG")
if [ "$EGRESS_COUNT_2" -ne 5 ]; then
    echo "FAIL: expected exactly 5 egress lines total after failover (4 + 1 more ACCEPTED), got $EGRESS_COUNT_2"
    echo "--- NetworkServiceApp log ---"
    cat "$NET_LOG"
    exit 1
fi

echo "PASS: failover complete, survivor's egress reached the gateway ($EGRESS_COUNT_2 total egress lines)"
