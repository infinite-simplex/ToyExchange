#pragma once
#include <chrono>
#include <cstdint>

using Price = std::uint8_t;
using Quantity = std::int32_t;
using OrderId = std::uint64_t;
constexpr OrderId INVALID_ORDER_ID = 0; // real ids are assigned starting at 1
using ExecutionId = std::uint64_t;
using FirmId = std::uint32_t;
using PoolIdx = std::uint32_t;
using Clock = std::chrono::steady_clock;
using TraceId = std::uint32_t; // Wraps around every 4.2 billion orders

// Nanoseconds since the TAI epoch (see get_synced_time_ns() in
// Telemetry.hpp). Deliberately not a std::chrono::steady_clock::time_point:
// steady_clock's epoch is implementation-defined and arbitrary per machine
// (typically boot time), so it has exactly the same cross-machine
// incomparability problem as a raw TSC value — an OrderEvent stamped on one
// machine and read/compared on another needs a real, synchronized wall-clock
// reading. (std::chrono::tai_clock, the typed C++20 equivalent, is declared
// but not implemented on this toolchain — confirmed via a direct compile
// check against libstdc++ 11 — hence the plain integer instead.)
using Timestamp = std::uint64_t;

// Opaque per-connection correlation tag, minted once by NetworkGateway when a
// client connects and stamped onto every command it sends. Carried through
// OuchOrderCommand -> Order -> OrderEvent unchanged (same pattern as
// FirmId/TraceId) so egress can route a response back to the right client
// without a side-table order_id -> connection lookup, which would go stale
// for long-resting orders once a fd gets closed and reused. See
// NetworkGateway::sendToSession.
using SessionId = std::uint32_t;
constexpr SessionId INVALID_SESSION_ID = 0; // real session ids are assigned starting at 1

// Static, operator-assigned identity for a MatchingService replica (see
// NetworkService/Arbitration/). Small range — only ever a handful of
// replicas — unlike SessionId, which churns per client connection.
using ReplicaId = std::uint16_t;
constexpr ReplicaId INVALID_REPLICA_ID = 0; // real replica ids are assigned starting at 1

// Fencing token for matching-replica leader election. Bumped by
// ReplicaArbiter on every leadership change (including transitions to/from
// "no leader"), and stamped on every egress message so a stale/zombie
// former leader's late traffic can be dropped at the sink instead of
// requiring perfect failure detection. See ReplicaArbiter.hpp.
using FencingEpoch = std::uint64_t;
constexpr FencingEpoch NO_EPOCH = 0; // before any leader has ever been chosen
