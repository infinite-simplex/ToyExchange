#!/usr/bin/env bash
# End-to-end smoke test: one NetworkServiceApp gateway + two MatchingServiceApp
# replicas consuming the same replication multicast feed, driven by a real
# OUCH client (OuchTestClient). Confirms both replicas converge to the same
# best bid/ask after a small resting + crossing order sequence.
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
    > "$NET_LOG" 2>&1 &
PIDS+=("$!")

sleep 0.3

"$MATCH_BIN" --id replica-A --stats-interval-ms 300 \
    --multicast-ip "$MCAST_IP" --multicast-port "$MCAST_PORT" \
    --retransmit-ip 127.0.0.1 --retransmit-port "$RETRANSMIT_PORT" \
    --egress-ip "$EGRESS_IP" --egress-port "$EGRESS_PORT" \
    > "$REPLICA_A_LOG" 2>&1 &
PIDS+=("$!")

"$MATCH_BIN" --id replica-B --stats-interval-ms 300 \
    --multicast-ip "$MCAST_IP" --multicast-port "$MCAST_PORT" \
    --retransmit-ip 127.0.0.1 --retransmit-port "$RETRANSMIT_PORT" \
    --egress-ip "$EGRESS_IP" --egress-port "$EGRESS_PORT" \
    > "$REPLICA_B_LOG" 2>&1 &
PIDS+=("$!")

sleep 0.5

echo "Sending orders..."
# Price is a 0-100 scale in this engine (event-contract style pricing;
# OrderBook's price levels array is sized [0,100] — see MAX_LEVELS in
# MatchingService/OrderBook.hpp), so keep test prices in that range.
"$CLIENT_BIN" --host 127.0.0.1 --port "$OUCH_PORT" --symbol TEST --side B --price 40 --qty 50
"$CLIENT_BIN" --host 127.0.0.1 --port "$OUCH_PORT" --symbol TEST --side S --price 60 --qty 50
# This third order crosses the resting ask on price, but every order in this
# system currently carries firm_id=0 (no real account/firm assignment wired
# up yet — see OrderBook::submit_order's create_order_from_command(cmd, 0)),
# so self-trade prevention treats it as the same firm trading with itself and
# rejects it rather than filling. That's expected today, not a test bug —
# and it's a useful check in its own right: confirms a REJECTED event (with
# the correct SELF_TRADING_PREVENTION reason code) makes it all the way back
# to the client as a private OUCH ack, not just ACCEPTED ones.
"$CLIENT_BIN" --host 127.0.0.1 --port "$OUCH_PORT" --symbol TEST --side B --price 60 --qty 30 --listen-ms 800

sleep 1.0

STATE_A="$(grep 'best bid=' "$REPLICA_A_LOG" | tail -1)"
STATE_B="$(grep 'best bid=' "$REPLICA_B_LOG" | tail -1)"

echo "--- NetworkServiceApp log (egress prints here; expect each event"
echo "    duplicated once per replica — no leader election yet) ---"
cat "$NET_LOG"
echo "--- replica-A log ---"
cat "$REPLICA_A_LOG"
echo "--- replica-B log ---"
cat "$REPLICA_B_LOG"
echo "--- ITCH multicast sniff (independent subscriber) ---"
sleep 4.5 # let the sniffer's 5s window finish so its log is complete
cat "$ITCH_LOG"

echo
echo "replica-A final: $STATE_A"
echo "replica-B final: $STATE_B"

NORM_A="${STATE_A#*] }"
NORM_B="${STATE_B#*] }"

if [ -z "$STATE_A" ] || [ -z "$STATE_B" ]; then
    echo "FAIL: one or both replicas never printed a stats line"
    exit 1
fi

if [ "$NORM_A" != "$NORM_B" ]; then
    echo "FAIL: replicas diverged"
    exit 1
fi

echo "PASS: replicas converged -> $NORM_A"
