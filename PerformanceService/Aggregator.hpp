#pragma once
#include "Alias.hpp"
#include "TelemetryReport.hpp"
#include <hdr/hdr_histogram.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <unordered_map>

enum class MetricKind : std::uint8_t {
    EngineExecutionNs = 0, // match_done_tai_ns - engine_pop_tai_ns — PRIMARY metric,
                            // the matching engine's own execution time, unaffected by
                            // whether the order ends up resting afterward.
    NetworkDelayNs,         // replica_ingress_tai_ns - ingress_tai_ns
    QueuingDelayNs,         // engine_pop_tai_ns - replica_ingress_tai_ns
    InboundQDepth,          // diagnostic: SPSC depth at push time
    InboundQSubmitCycles,   // diagnostic: LOCAL rdtsc cycles — only meaningful as a
                            // trend for one replica over time, never comparable in
                            // absolute terms across replicas/machines.
    PriceLevelsTouched,     // "why was it slow" diagnostic
    RestingOrdersTouched,   // "why was it slow" diagnostic
    Count
};
constexpr std::size_t METRIC_COUNT = static_cast<std::size_t>(MetricKind::Count);
constexpr std::size_t metric_index(MetricKind k) { return static_cast<std::size_t>(k); }

// One rolling metric: RETAINED_WINDOWS+1 hdr_histograms recycled as a ring
// buffer. m_active is the window currently being written to; rollover()
// advances the ring and hdr_resets the slot being reclaimed — O(1), zero
// allocation after startup, matching HdrHistogram's own idiom for "close a
// window and start fresh."
//
// Not thread-safe by design: everything here is owned and called from
// TelemetryReceiver's single recv-loop thread. HdrHistogram_c's plain
// hdr_record_value/hdr_reset aren't documented safe for concurrent use (the
// library ships a separate, atomic-specific variant for that) — a second
// thread here would be unjustified complexity for a loop that already wakes
// at least every 200ms via its own recv timeout, plenty precise for 1s
// window granularity.
class WindowedHistogram {
public:
    static constexpr int RETAINED_WINDOWS = 60; // ~60s of history at the default 1s window

    WindowedHistogram() = default;
    ~WindowedHistogram();
    WindowedHistogram(const WindowedHistogram&) = delete;
    WindowedHistogram& operator=(const WindowedHistogram&) = delete;

    void init(std::int64_t lowestTrackable, std::int64_t highestTrackable, int significantFigures);

    void record(std::int64_t value) { hdr_record_value(m_ring[m_active], value); }

    // Called once per elapsed window (see Aggregator::maybeRollover).
    void rollover();

    const hdr_histogram* mostRecentlyClosed() const {
        return m_ring[(m_active + RETAINED_WINDOWS) % (RETAINED_WINDOWS + 1)];
    }

    // Merges every retained closed window plus the still-filling active one
    // into a scratch histogram for a genuinely smoothed rolling view — cheap
    // (O(bucket count), not O(sample count), the actual reason HdrHistogram
    // beats hand-rolling this). Also smooths over a real quirk:
    // on_trace_complete only fires on order eviction, so a resting order's
    // sample can land several windows after its actual engine-execution
    // timestamp — a single window's percentile isn't a clean "last second,"
    // but the merged view is far less sensitive to that lag.
    const hdr_histogram* rolling() {
        hdr_reset(m_scratch);
        for (int i = 0; i <= RETAINED_WINDOWS; ++i) {
            hdr_add(m_scratch, m_ring[(m_active + i) % (RETAINED_WINDOWS + 1)]);
        }
        return m_scratch;
    }

private:
    std::array<hdr_histogram*, RETAINED_WINDOWS + 1> m_ring{};
    hdr_histogram* m_scratch{ nullptr };
    int m_active{ 0 };
};

struct ReplicaStats {
    ReplicaStats();
    std::array<WindowedHistogram, METRIC_COUNT> metrics;
};

// Single-threaded (see WindowedHistogram's doc comment) aggregator: routes
// each received TelemetryReport into the right replica's histograms, and
// rolls every replica's windows over together on one shared timer so
// replicas stay directly comparable window-for-window.
class Aggregator {
public:
    static constexpr auto WINDOW = std::chrono::milliseconds(1000);

    void record(const TelemetryReport& report);

    // Takes `now` explicitly (rather than reading steady_clock internally)
    // so tests can drive rollover deterministically without sleeping real
    // time per window. Returns true if a rollover just happened — callers
    // should dump the just-closed window on that signal.
    bool maybeRollover(std::chrono::steady_clock::time_point now,
                        std::chrono::milliseconds window = WINDOW);

    void dumpToConsole() const;

    // Only safe to call from record()'s own thread (or after that thread has
    // been stopped/joined) — the map itself has no internal synchronization,
    // matching Aggregator's single-threaded-by-design contract.
    const std::unordered_map<ReplicaId, ReplicaStats>& byReplica() const { return m_byReplica; }

    // Thread-safe unlike byReplica(): a plain count of record() calls so far,
    // for a caller on another thread (e.g. a test) to know processing has
    // caught up to a point, without touching the map itself.
    std::uint64_t recordCount() const noexcept { return m_recordCount.load(std::memory_order_acquire); }

private:
    std::unordered_map<ReplicaId, ReplicaStats> m_byReplica;
    std::chrono::steady_clock::time_point m_windowStart{};
    bool m_started{ false };
    std::atomic<std::uint64_t> m_recordCount{ 0 };
};
