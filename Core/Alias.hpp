#pragma once
#include <cstdint>

using TraceId = std::uint32_t; // Wraps around every 4.2 billion orders
constexpr TraceId INVALID_TRACE_ID = 0xFFFFFFFFu; // trace_id 0 is a real, assigned value
                                                   // (the very first command), so unlike
                                                   // OrderId/SessionId this can't reuse 0

// Opaque per-connection correlation tag, minted once by NetworkGateway when a
// client connects and stamped onto every command it sends. Carried unchanged
// from a product's inbound command through to its outbound event (same
// pattern as TraceId) so egress can route a response back to the right
// client without a side-table lookup, which would go stale for long-resting
// orders once a fd gets closed and reused. See NetworkGateway::sendToSession.
using SessionId = std::uint32_t;
constexpr SessionId INVALID_SESSION_ID = 0; // real session ids are assigned starting at 1

// Static, operator-assigned identity for a MatchingService replica (see
// Core/Arbitration/). Small range — only ever a handful of replicas —
// unlike SessionId, which churns per client connection.
using ReplicaId = std::uint16_t;
constexpr ReplicaId INVALID_REPLICA_ID = 0; // real replica ids are assigned starting at 1

// Fencing token for matching-replica leader election. Bumped by
// ReplicaArbiter on every leadership change (including transitions to/from
// "no leader"), and stamped on every egress message so a stale/zombie
// former leader's late traffic can be dropped at the sink instead of
// requiring perfect failure detection. See ReplicaArbiter.hpp.
using FencingEpoch = std::uint64_t;
constexpr FencingEpoch NO_EPOCH = 0; // before any leader has ever been chosen
