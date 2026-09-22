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
using Timestamp = std::chrono::steady_clock::time_point;

// Opaque per-connection correlation tag, minted once by NetworkGateway when a
// client connects and stamped onto every command it sends. Carried through
// OuchOrderCommand -> Order -> OrderEvent unchanged (same pattern as
// FirmId/TraceId) so egress can route a response back to the right client
// without a side-table order_id -> connection lookup, which would go stale
// for long-resting orders once a fd gets closed and reused. See
// NetworkGateway::sendToSession.
using SessionId = std::uint32_t;
constexpr SessionId INVALID_SESSION_ID = 0; // real session ids are assigned starting at 1
