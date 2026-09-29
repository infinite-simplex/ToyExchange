#include <gtest/gtest.h>
#include "ReplicaArbiter.hpp"
#include "HeartbeatMessage.hpp"
#include "Telemetry.hpp"
#include "TelemetryForwarder.hpp"
#include "TelemetryReceiver.hpp"
#include "TelemetryReport.hpp"
#include "Aggregator.hpp"
#include "NetworkConfig.hpp"
#include "SPSCQueue.hpp"

#include <chrono>
#include <ctime>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

// =====================================================================
// ReplicaArbiter — matching-replica leader election / fencing
// =====================================================================

namespace {

HeartbeatResponse SendHeartbeat(uint16_t arbiterPort, ReplicaId id, uint64_t lastAppliedSeq) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    timeval rxTimeout{};
    rxTimeout.tv_sec = 0;
    rxTimeout.tv_usec = 500'000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &rxTimeout, sizeof(rxTimeout));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(arbiterPort);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    HeartbeatRequest req{ id, lastAppliedSeq };
    sendto(sock, &req, sizeof(req), 0, (sockaddr*)&addr, sizeof(addr));

    HeartbeatResponse resp{};
    recv(sock, &resp, sizeof(resp), 0);
    close(sock);
    return resp;
}

} // namespace

TEST(ReplicaArbiterTest, FirstEverHeartbeatBecomesLeaderWithEpochOne) {
    ReplicaArbiter arbiter(46001, std::chrono::milliseconds(50));
    arbiter.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    auto resp = SendHeartbeat(46001, 5, 0);

    EXPECT_EQ(ReplicaId(5), resp.leaderId);
    EXPECT_EQ(FencingEpoch(1), resp.epoch); // NO_EPOCH (0) -> 1 on the very first election

    arbiter.stop();
}

TEST(ReplicaArbiterTest, StickyLeadershipDoesNotReclaim) {
    ReplicaArbiter arbiter(46002, std::chrono::milliseconds(50));
    arbiter.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // Replica 2 heartbeats alone first and becomes leader.
    auto first = SendHeartbeat(46002, 2, 0);
    EXPECT_EQ(ReplicaId(2), first.leaderId);

    // Replica 1 (lower id) shows up shortly after, while 2 is still fresh.
    // Sticky: leader must stay 2, and the epoch must NOT bump — a lower id
    // reappearing is not itself a leadership change.
    auto second = SendHeartbeat(46002, 1, 0);
    EXPECT_EQ(ReplicaId(2), second.leaderId);
    EXPECT_EQ(first.epoch, second.epoch);

    arbiter.stop();
}

TEST(ReplicaArbiterTest, EpochBumpsWhenLeaderFailsOver) {
    ReplicaArbiter arbiter(46003, std::chrono::milliseconds(30));
    arbiter.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    auto first = SendHeartbeat(46003, 1, 0);
    EXPECT_EQ(ReplicaId(1), first.leaderId);

    // Let replica 1 go stale (stop heartbeating) past the staleness window;
    // replica 2 checking in afterward should take over with a higher epoch.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    auto second = SendHeartbeat(46003, 2, 0);

    EXPECT_EQ(ReplicaId(2), second.leaderId);
    EXPECT_GT(second.epoch, first.epoch);

    arbiter.stop();
}

TEST(ReplicaArbiterTest, LowestIdWinsAmongEligibleCandidatesOnFailover) {
    ReplicaArbiter arbiter(46004, std::chrono::milliseconds(150));
    arbiter.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // Replica 3 becomes leader alone first.
    auto initial = SendHeartbeat(46004, 3, 0);
    EXPECT_EQ(ReplicaId(3), initial.leaderId);

    // Replicas 1 and 2 check in while 3 is still comfortably fresh — both
    // become known to the arbiter, but sticky leadership keeps 3 in charge
    // (recompute happens per-request using ALL tracked replicas' last known
    // status, not just the requester's — this is what makes the eventual
    // tie-break below possible: replica 1's freshness here persists in the
    // arbiter's map even though a later, different replica's heartbeat is
    // what actually triggers noticing 3 went stale).
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto stillSticky1 = SendHeartbeat(46004, 1, 0);
    auto stillSticky2 = SendHeartbeat(46004, 2, 0);
    EXPECT_EQ(ReplicaId(3), stillSticky1.leaderId);
    EXPECT_EQ(ReplicaId(3), stillSticky2.leaderId);

    // Replica 3 goes silent for good. Once its staleness window elapses,
    // replica 2 re-checks in (refreshing only itself) — replica 1's earlier
    // check-in above is still well within the staleness window at this
    // point, so both 1 and 2 are eligible, and the lowest id should win
    // regardless of which one's request triggered this recompute.
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    auto afterFailover = SendHeartbeat(46004, 2, 0);

    EXPECT_EQ(ReplicaId(1), afterFailover.leaderId);
    EXPECT_GT(afterFailover.epoch, initial.epoch);

    arbiter.stop();
}
// =====================================================================
// get_synced_time_ns() — cross-machine-comparable telemetry timestamps
// =====================================================================
//
// What's realistically testable here: that the primitive itself returns
// real wall time (not an arbitrary local counter) and is monotonic across
// rapid successive calls. Genuinely simulating two *unsynchronized*
// machines isn't practical as a unit test without a mockable clock source
// — that's a bigger change than this primitive needs, and out of scope.

TEST(TelemetryTest, SyncedTimeMatchesRealWallClock) {
    // time(nullptr) is seconds since the Unix epoch (UTC); CLOCK_TAI is
    // currently 37 seconds ahead of UTC (the accumulated leap-second
    // count) and drifts further only on the rare future leap second, so a
    // wide tolerance comfortably covers both that fixed offset and any
    // scheduling jitter between the two calls without the test needing to
    // hardcode the current leap-second count.
    uint64_t before = static_cast<uint64_t>(::time(nullptr)) * 1'000'000'000ull;
    uint64_t synced = get_synced_time_ns();

    constexpr uint64_t ONE_MINUTE_NS = 60ull * 1'000'000'000ull;
    uint64_t diff = (synced > before) ? (synced - before) : (before - synced);
    EXPECT_LT(diff, ONE_MINUTE_NS)
        << "get_synced_time_ns() should be within real wall-clock range, not an arbitrary counter";
}

TEST(TelemetryTest, SyncedTimeIsMonotonicAcrossRapidCalls) {
    uint64_t previous = get_synced_time_ns();
    for (int i = 0; i < 1000; ++i) {
        uint64_t current = get_synced_time_ns();
        EXPECT_GE(current, previous)
            << "get_synced_time_ns() went backward between two calls a moment apart";
        previous = current;
    }
}

// =====================================================================
// PerformanceService — WindowedHistogram / Aggregator / wire path
// =====================================================================

TEST(WindowedHistogramTest, RecordAndRolloverReportsClosedWindowPercentiles) {
    WindowedHistogram h;
    h.init(1, 1'000'000, 3);

    h.record(100);
    h.record(200);
    h.record(300);

    // Nothing has rolled over yet — "most recently closed" is still the
    // untouched slot init() left empty, not the one being written to.
    EXPECT_EQ(0, h.mostRecentlyClosed()->total_count);

    h.rollover();

    const hdr_histogram* closed = h.mostRecentlyClosed();
    EXPECT_EQ(3, closed->total_count);
    EXPECT_EQ(100, hdr_min(closed));
    EXPECT_EQ(300, hdr_max(closed));

    // The newly active window must be empty — rollover() has to hdr_reset
    // the slot it just claimed, not just advance the pointer past it.
    h.record(999);
    EXPECT_EQ(3, h.mostRecentlyClosed()->total_count)
        << "recording into the new window must not touch the just-closed one";
}

TEST(WindowedHistogramTest, RollingMergesRetainedAndActiveWindows) {
    WindowedHistogram h;
    h.init(1, 1'000'000, 3);

    h.record(10);
    h.rollover();
    h.record(20);
    h.rollover();
    h.record(30); // still in the active, not-yet-closed window

    const hdr_histogram* merged = h.rolling();
    EXPECT_EQ(3, merged->total_count);
    EXPECT_EQ(10, hdr_min(merged));
    EXPECT_EQ(30, hdr_max(merged));
}

TEST(AggregatorTest, RecordRoutesToCorrectReplicaWithoutCrossContamination) {
    Aggregator agg;

    TelemetryReport reportA{};
    reportA.replicaId = 1;
    reportA.trace.engine_pop_tai_ns = 1'000;
    reportA.trace.match_done_tai_ns = 1'500; // engine exec = 500ns

    TelemetryReport reportB{};
    reportB.replicaId = 2;
    reportB.trace.engine_pop_tai_ns = 2'000;
    reportB.trace.match_done_tai_ns = 9'000; // engine exec = 7000ns

    agg.record(reportA);
    agg.record(reportB);

    auto t0 = std::chrono::steady_clock::now();
    EXPECT_FALSE(agg.maybeRollover(t0)) << "the first call just establishes the window start";
    EXPECT_TRUE(agg.maybeRollover(t0 + std::chrono::seconds(2), std::chrono::milliseconds(1000)));

    const auto& byReplica = agg.byReplica();
    ASSERT_EQ(size_t(1), byReplica.count(1));
    ASSERT_EQ(size_t(1), byReplica.count(2));

    const hdr_histogram* engineA =
        byReplica.at(1).metrics[metric_index(MetricKind::EngineExecutionNs)].mostRecentlyClosed();
    const hdr_histogram* engineB =
        byReplica.at(2).metrics[metric_index(MetricKind::EngineExecutionNs)].mostRecentlyClosed();

    EXPECT_EQ(1, engineA->total_count);
    EXPECT_EQ(500, hdr_min(engineA));
    EXPECT_EQ(1, engineB->total_count);
    EXPECT_EQ(7000, hdr_min(engineB));
}

TEST(AggregatorTest, IgnoresIncompleteTimestampPairs) {
    Aggregator agg;

    TelemetryReport report{};
    report.replicaId = 1;
    report.trace.engine_pop_tai_ns = 0; // never populated, e.g. a reject path
    report.trace.match_done_tai_ns = 5000;

    agg.record(report);

    auto t0 = std::chrono::steady_clock::now();
    agg.maybeRollover(t0);
    agg.maybeRollover(t0 + std::chrono::seconds(2), std::chrono::milliseconds(1000));

    const hdr_histogram* engine =
        agg.byReplica().at(1).metrics[metric_index(MetricKind::EngineExecutionNs)].mostRecentlyClosed();
    EXPECT_EQ(0, engine->total_count)
        << "a zeroed timestamp must not produce a bogus underflowed delta";
}

// End-to-end: TelemetryForwarder -> real UDP socket -> TelemetryReceiver ->
// Aggregator. The only test that can catch a struct-layout mismatch between
// sender and receiver — both sides could pass their own isolated unit tests
// while disagreeing with each other, since that only shows up via an actual
// memcpy over a socket. Matches this file's existing precedent of wire-
// testing NetworkGateway/MulticastIngressReceiver end-to-end.
TEST(PerformanceServiceWireTest, ForwarderReachesReceiverAndAggregator) {
    SPSCQueue<OrderTrace, 16> sourceQueue;

    NetworkConfig config;
    config.performanceServiceIp = "127.0.0.1";
    config.performanceServicePort = 45201;

    Aggregator aggregator;
    TelemetryReceiver telemetryReceiver(aggregator, config.performanceServicePort);
    telemetryReceiver.start();

    TelemetryForwarder<16> forwarder(sourceQueue, config, ReplicaId(7));
    forwarder.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    OrderTrace trace{};
    trace.engine_pop_tai_ns = 1'000;
    trace.match_done_tai_ns = 1'250; // engine exec = 250ns
    ASSERT_TRUE(sourceQueue.try_push(trace));

    // recordCount() is the thread-safe part of Aggregator (see its header
    // comment) — safe to poll from this thread while TelemetryReceiver's
    // recv-loop thread is still running.
    bool arrived = false;
    for (int i = 0; i < 100 && !arrived; ++i) {
        arrived = aggregator.recordCount() >= 1;
        if (!arrived) std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(arrived) << "TelemetryReport never made it from forwarder to aggregator";

    // Only safe to read byReplica()/query histograms once the only thread
    // that ever writes to them has been stopped — see Aggregator::byReplica's
    // doc comment.
    forwarder.stop();
    telemetryReceiver.stop();

    auto t0 = std::chrono::steady_clock::now();
    aggregator.maybeRollover(t0);
    aggregator.maybeRollover(t0 + std::chrono::seconds(2), std::chrono::milliseconds(1000));

    const hdr_histogram* engine = aggregator.byReplica().at(ReplicaId(7))
        .metrics[metric_index(MetricKind::EngineExecutionNs)].mostRecentlyClosed();
    EXPECT_EQ(1, engine->total_count);
    EXPECT_EQ(250, hdr_min(engine));
}
