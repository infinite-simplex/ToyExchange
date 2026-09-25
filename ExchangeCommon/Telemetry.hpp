#pragma once
#include <cstdint>
#include <cstddef>
#include <chrono>
#include <ctime>
#include <x86intrin.h>
#include "Alias.hpp"

struct alignas(64) OrderTrace {
    // Written by Ingress Server Thread. ingress_tai_ns is stamped on the
    // gateway's machine and travels over the wire into a replica's own
    // arena slot, so it (and everything it's later diffed against) must be
    // a real, cross-machine-comparable wall-clock reading, not a raw TSC
    // cycle count — see get_synced_time_ns() below.
    std::uint64_t ingress_tai_ns{ 0 };         // Nanoseconds since the TAI epoch at socket read
    std::uint32_t inbound_q_depth{ 0 };        // SPSC depth at push time
    std::uint64_t inbound_q_submitted{ 0 };    // time taken to add to the queue — purely local
                                                // (both reads happen on the same machine), so this
                                                // one stays on raw get_time() cycles; only fields
                                                // compared across a machine boundary need get_synced_time_ns().
    // Written by Matching Engine Thread
    std::uint64_t engine_pop_tai_ns{ 0 };      // Nanoseconds since the TAI epoch when popped from queue
    std::uint64_t match_done_tai_ns{ 0 };      // Nanoseconds since the TAI epoch after LOB execution
#ifdef ENABLE_DETAILED_TELEMETRY
    std::uint32_t resting_orders_touched{ 0 }; // Number of fills generated
    std::uint16_t price_levels_touched{ 0 };   // Number of price levels traversed
#endif
};

// Raw TSC cycles — cheap, high-resolution, but only ever meaningful as a
// LOCAL relative delta between two reads on the same machine (and ideally
// the same core; RDTSCP's discarded second output would flag a core
// migration, but nothing here currently checks it). Never compare a value
// from this function against one read on a different machine.
static inline uint64_t get_time() noexcept {
    unsigned int tmp;
    return __rdtscp(&tmp);
}

// Nanoseconds since the TAI epoch, via clock_gettime(CLOCK_TAI). Use this
// (not get_time()) for any timestamp that might ever be compared across a
// process/machine boundary — e.g. a value stamped on NetworkGateway's host
// and later diffed against one stamped on a MatchingService replica's host.
// TAI specifically, not CLOCK_REALTIME: TAI has no leap seconds, so it
// can't step backward the way CLOCK_REALTIME occasionally does even under
// a correctly-running time-sync daemon — a backward step here would
// silently reintroduce the same negative-delta problem this function
// exists to prevent. Correctness (i.e. this value actually agreeing with
// another machine's) depends on a running chrony/ptp4l instance
// disciplining the kernel clock — see Tools/ptp/.
static inline uint64_t get_synced_time_ns() noexcept {
    struct timespec ts;
    clock_gettime(CLOCK_TAI, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull + static_cast<uint64_t>(ts.tv_nsec);
}

// Sized for ~1M orders/day (rounded up to a power of 2) so a full trading
// day's worth of trace_ids never wrap around and clobber unflushed data.
// A day boundary is just the ring naturally recycling slots that (by then)
// should already have been drained out via SPSCProducerPolicy::on_trace_complete
// and persisted; there is no separate reset step.
constexpr std::size_t TELEMETRY_POOL_SIZE = 1'048'576; // 2^20
static_assert((TELEMETRY_POOL_SIZE & (TELEMETRY_POOL_SIZE - 1)) == 0,
    "TELEMETRY_POOL_SIZE must be a power of 2 for trace_index()'s masking");
alignas(64) inline OrderTrace g_telemetry_arena[TELEMETRY_POOL_SIZE];

// The single canonical way to turn a TraceId into an arena slot. Every
// read/write of g_telemetry_arena must go through this — never index it
// directly with a raw id, or two call sites can silently disagree on which
// slot belongs to a given order (or index out of bounds entirely).
inline std::size_t trace_index(TraceId id) noexcept {
    return static_cast<std::size_t>(id) & (TELEMETRY_POOL_SIZE - 1);
}