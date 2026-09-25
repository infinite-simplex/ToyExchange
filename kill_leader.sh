#!/usr/bin/env bash
# Finds whichever MatchingServiceApp replica launched by run_exchange.sh is
# currently the elected leader (per its own periodic stats line) and kills
# just that one. This is the actual way to test failover: kill the leader,
# watch a standby take over — as opposed to pkill-ing every replica at once,
# which tests total outage instead, since nothing is left to fail over to.
# Mirrors the same find-the-leader-then-kill-it logic as
# Tools/two_replica_smoke_test.sh.
#
# Usage (run in a separate terminal while run_exchange.sh is up):
#   ./kill_leader.sh [run-dir]
set -uo pipefail

RUN_DIR="${1:-run}"

if ! ls "$RUN_DIR"/replica-*.log >/dev/null 2>&1; then
    echo "No replica logs found in $RUN_DIR/ — is run_exchange.sh running?" >&2
    exit 1
fi

LEADERS=()
for log in "$RUN_DIR"/replica-*.log; do
    replica_num="$(basename "$log" .log | sed -E 's/replica-//')"
    if tail -1 "$log" | grep -q 'leader=yes'; then
        LEADERS+=("$replica_num")
    fi
done

if [ "${#LEADERS[@]}" -eq 0 ]; then
    echo "No replica currently reports leader=yes — check $RUN_DIR/replica-*.log" \
         "(heartbeats may not have established a leader yet)." >&2
    exit 1
fi

if [ "${#LEADERS[@]}" -gt 1 ]; then
    echo "Warning: more than one replica reports leader=yes (${LEADERS[*]}) —" \
         "likely caught mid-failover. Killing the first one found." >&2
fi

LEADER_NUM="${LEADERS[0]}"
# Trailing space after the number disambiguates replica-id 1 from 10, 11, ...
LEADER_PID="$(pgrep -f "replica-id $LEADER_NUM ")"

if [ -z "$LEADER_PID" ]; then
    echo "replica-$LEADER_NUM's log says it's the leader, but no matching process" \
         "was found (already exited?)." >&2
    exit 1
fi

echo "Leader is replica-$LEADER_NUM (pid $LEADER_PID) — killing it to trigger failover..."
kill "$LEADER_PID"
echo "Sent. Watch the survivors pick up leadership with a higher epoch:"
echo "  tail -f $RUN_DIR/replica-*.log | grep --line-buffered leader="
