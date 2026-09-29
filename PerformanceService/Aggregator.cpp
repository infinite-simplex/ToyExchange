#include "Aggregator.hpp"
#include <iostream>

namespace {

struct MetricRange {
    std::int64_t low;
    std::int64_t high;
    int significantFigures;
};

// Centralized per-metric hdr_init ranges/precision — the single source for
// these, rather than scattering the literals across call sites.
constexpr MetricRange kMetricRanges[METRIC_COUNT] = {
    { 1, 1'000'000'000LL, 3 },   // EngineExecutionNs — up to 1s
    { 1, 10'000'000'000LL, 3 },  // NetworkDelayNs — up to 10s
    { 1, 10'000'000'000LL, 3 },  // QueuingDelayNs — up to 10s
    { 1, 2'048, 2 },              // InboundQDepth
    { 1, 10'000'000LL, 3 },       // InboundQSubmitCycles — raw rdtsc cycles
    // Generic range for an opaque product counter — wide enough for a plain
    // count (a product's own values are typically small integers), not
    // tuned to any specific meaning since Core doesn't know one.
    { 1, 1'000'000, 2 },  // ExtCounter0
    { 1, 1'000'000, 2 },  // ExtCounter1
    { 1, 1'000'000, 2 },  // ExtCounter2
    { 1, 1'000'000, 2 },  // ExtCounter3
};

constexpr const char* kMetricNames[METRIC_COUNT] = {
    "engine_exec_ns", "network_delay_ns", "queuing_delay_ns",
    "inbound_q_depth", "inbound_q_submit_cycles",
    "ext_counter_0", "ext_counter_1", "ext_counter_2", "ext_counter_3"
};

} // namespace

WindowedHistogram::~WindowedHistogram() {
    for (auto* h : m_ring) {
        if (h) hdr_close(h);
    }
    if (m_scratch) hdr_close(m_scratch);
}

void WindowedHistogram::init(std::int64_t lowestTrackable, std::int64_t highestTrackable, int significantFigures) {
    for (auto& h : m_ring) {
        hdr_init(lowestTrackable, highestTrackable, significantFigures, &h);
    }
    hdr_init(lowestTrackable, highestTrackable, significantFigures, &m_scratch);
}

void WindowedHistogram::rollover() {
    m_active = (m_active + 1) % (RETAINED_WINDOWS + 1);
    hdr_reset(m_ring[m_active]);
}

ReplicaStats::ReplicaStats() {
    for (std::size_t i = 0; i < METRIC_COUNT; ++i) {
        metrics[i].init(kMetricRanges[i].low, kMetricRanges[i].high, kMetricRanges[i].significantFigures);
    }
}

void Aggregator::record(const TelemetryReport& report) {
    ReplicaStats& stats = m_byReplica[report.replicaId]; // default-constructs (and inits) on first sight
    const OrderTrace& t = report.trace;

    // Guarded on both timestamps being non-zero and causally ordered — some
    // reject paths complete a trace without every field ever populated, and
    // an unsigned underflow here would silently corrupt a histogram with a
    // huge bogus value.
    if (t.match_done_tai_ns != 0 && t.engine_pop_tai_ns != 0 && t.match_done_tai_ns >= t.engine_pop_tai_ns) {
        stats.metrics[metric_index(MetricKind::EngineExecutionNs)]
            .record(static_cast<std::int64_t>(t.match_done_tai_ns - t.engine_pop_tai_ns));
    }
    if (t.replica_ingress_tai_ns != 0 && t.ingress_tai_ns != 0 && t.replica_ingress_tai_ns >= t.ingress_tai_ns) {
        stats.metrics[metric_index(MetricKind::NetworkDelayNs)]
            .record(static_cast<std::int64_t>(t.replica_ingress_tai_ns - t.ingress_tai_ns));
    }
    if (t.engine_pop_tai_ns != 0 && t.replica_ingress_tai_ns != 0 && t.engine_pop_tai_ns >= t.replica_ingress_tai_ns) {
        stats.metrics[metric_index(MetricKind::QueuingDelayNs)]
            .record(static_cast<std::int64_t>(t.engine_pop_tai_ns - t.replica_ingress_tai_ns));
    }
    stats.metrics[metric_index(MetricKind::InboundQDepth)].record(static_cast<std::int64_t>(t.inbound_q_depth));
    stats.metrics[metric_index(MetricKind::InboundQSubmitCycles)].record(static_cast<std::int64_t>(t.inbound_q_submitted));

    constexpr std::size_t firstExtSlot = metric_index(MetricKind::ExtCounter0);
    for (std::size_t i = 0; i < t.ext_counters.size(); ++i) {
        stats.metrics[firstExtSlot + i].record(static_cast<std::int64_t>(t.ext_counters[i]));
    }

    // Released last, after every map/histogram write above — a caller on
    // another thread that observes this count (acquire) is guaranteed to see
    // this record's effects too, e.g. a test polling recordCount() before
    // stopping the receiver thread and only then safely reading byReplica().
    m_recordCount.fetch_add(1, std::memory_order_release);
}

bool Aggregator::maybeRollover(std::chrono::steady_clock::time_point now, std::chrono::milliseconds window) {
    if (!m_started) {
        m_windowStart = now;
        m_started = true;
        return false;
    }
    if (now - m_windowStart < window) return false;

    m_windowStart = now;
    for (auto& [id, stats] : m_byReplica) {
        for (auto& metric : stats.metrics) metric.rollover();
    }
    return true;
}

void Aggregator::dumpToConsole() const {
    for (const auto& [replicaId, stats] : m_byReplica) {
        for (std::size_t i = 0; i < METRIC_COUNT; ++i) {
            const hdr_histogram* h = stats.metrics[i].mostRecentlyClosed();
            if (h->total_count == 0) continue; // nothing recorded in this window yet — skip the noise

            std::cout << "[perf] replica=" << replicaId << " metric=" << kMetricNames[i]
                      << " n=" << h->total_count
                      << " min=" << hdr_min(h)
                      << " max=" << hdr_max(h)
                      << " p50=" << hdr_value_at_percentile(h, 50.0)
                      << " p99=" << hdr_value_at_percentile(h, 99.0)
                      << " p99.99=" << hdr_value_at_percentile(h, 99.99)
                      << "\n";
        }
    }
}
