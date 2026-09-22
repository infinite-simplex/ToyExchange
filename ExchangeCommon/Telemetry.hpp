#pragma once
#include <cstdint>
#include <cstddef>
#include <chrono>
#include <x86intrin.h>
#include "Alias.hpp"

struct alignas(64) OrderTrace {
    // Written by Ingress Server Thread
    std::uint64_t ingress_tsc{ 0 };            // Hardware cycle counter at socket read
    std::uint32_t inbound_q_depth{ 0 };        // SPSC depth at push time
    std::uint64_t inbound_q_submitted{ 0 };    // time taken to add to the queue
    // Written by Matching Engine Thread
    std::uint64_t engine_pop_tsc{ 0 };         // Cycle counter when popped from queue
    std::uint64_t match_done_tsc{ 0 };         // Cycle counter after LOB execution
#ifdef ENABLE_DETAILED_TELEMETRY
    std::uint32_t resting_orders_touched{ 0 }; // Number of fills generated
    std::uint16_t price_levels_touched{ 0 };   // Number of price levels traversed
#endif
};

static inline uint64_t get_time() noexcept {
    unsigned int tmp;
    return __rdtscp(&tmp);
}

// Sized for ~1M orders/day (rounded up to a power of 2) so a full trading
// day's worth of trace_ids never wrap around and clobber unflushed data.
// A day boundary is just the ring naturally recycling slots that (by then)
// should already have been drained out via SPSCProducerPolicy::on_trace_complete
// and persisted; there is no separate reset step.
constexpr std::size_t TELEMETRY_POOL_SIZE = 1'048'576; // 2^20
static_assert((TELEMETRY_POOL_SIZE & (TELEMETRY_POOL_SIZE - 1)) == 0,
    "TELEMETRY_POOL_SIZE must be a power of 2 for trace_index()'s masking");
inline alignas(64) OrderTrace g_telemetry_arena[TELEMETRY_POOL_SIZE];

// The single canonical way to turn a TraceId into an arena slot. Every
// read/write of g_telemetry_arena must go through this — never index it
// directly with a raw id, or two call sites can silently disagree on which
// slot belongs to a given order (or index out of bounds entirely).
inline std::size_t trace_index(TraceId id) noexcept {
    return static_cast<std::size_t>(id) & (TELEMETRY_POOL_SIZE - 1);
}