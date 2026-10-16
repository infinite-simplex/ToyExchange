// Egress: how a result gets from a replica to a client and to the public feed.
//
// Core has no event type or wire encoding of its own, so these tests use a minimal stand-in
// event (TestEvent) and encoder (TestEncoder), and a gateway that records what it is asked to
// send (RecordingGateway). Everything else is real: EgressPublisher, EgressGateway,
// ReplicaArbiter, LeaderHeartbeatClient and the multicast between them.
//
// What is checked: only the leader publishes; the gateway routes each event to its session and to
// the public feed in order; a message from an older epoch is dropped (fencing); and after the
// leader dies the survivor takes over while the dead leader's late messages are still dropped.
//
// What is not checked, because the code does not do it: egress has no gap-fill. A test that
// loses datagrams would only prove they stay lost.

#include <gtest/gtest.h>
#include "ArbiterTestSupport.hpp"
#include "EgressGateway.hpp"
#include "EgressPublisher.hpp"
#include "FencedMessage.hpp"
#include "LeaderHeartbeatClient.hpp"
#include "NetworkConfig.hpp"
#include "ReplicaArbiter.hpp"
#include "SPSCQueue.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

using arbiter_test::SendHeartbeat;

namespace {

constexpr auto GROUP_JOIN_WAIT = std::chrono::milliseconds(150);
constexpr auto NOTHING_ARRIVES_WAIT = std::chrono::milliseconds(300);

// --- the stand-ins -------------------------------------------------------------------------

struct TestEvent {
    SessionId session_id; // EgressGateway routes on this field
    uint64_t value;       // the test's own sequence, so order can be checked
};

struct TestEncoder {
    static constexpr size_t FRAME_BYTES = sizeof(uint64_t) + sizeof(SessionId);

    static std::array<char, FRAME_BYTES> encode(const TestEvent& event) {
        std::array<char, FRAME_BYTES> frame{};
        std::memcpy(frame.data(), &event.value, sizeof(event.value));
        std::memcpy(frame.data() + sizeof(event.value), &event.session_id, sizeof(event.session_id));
        return frame;
    }
    static void print(std::ostream& os, const TestEvent& event) {
        os << "session=" << event.session_id << " value=" << event.value;
    }
};

uint64_t FrameValue(const std::string& frame) {
    uint64_t value = 0;
    std::memcpy(&value, frame.data(), sizeof(value));
    return value;
}
struct Sent {
    SessionId session;
    std::string frame;
    bool delivered; // the session was connected
};

// Stands in for NetworkGateway on the egress side: only sendToSession matters here.
class RecordingGateway {
public:
    bool sendToSession(SessionId id, const char* data, size_t len) {
        std::lock_guard<std::mutex> lock(m_mutex);
        const bool connected = m_connected.count(id) != 0;
        m_sent.push_back({ id, std::string(data, len), connected });
        return connected;
    }
    void connect(SessionId id) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_connected.insert(id);
    }
    std::vector<Sent> sent() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_sent;
    }
    size_t sentCount() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_sent.size();
    }

private:
    mutable std::mutex m_mutex;
    std::vector<Sent> m_sent;
    std::set<SessionId> m_connected;
};

// A subscriber on the public feed group: records every datagram.
class PublicFeed {
public:
    explicit PublicFeed(const NetworkConfig& config) {
        m_fd = socket(AF_INET, SOCK_DGRAM, 0);
        int opt = 1;
        setsockopt(m_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        timeval timeout{};
        timeout.tv_usec = 50'000;
        setsockopt(m_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        sockaddr_in bindAddr{};
        bindAddr.sin_family = AF_INET;
        bindAddr.sin_port = htons(config.publicBroadcastPort);
        bindAddr.sin_addr.s_addr = INADDR_ANY;
        bind(m_fd, (sockaddr*)&bindAddr, sizeof(bindAddr));
        ip_mreq mreq{};
        inet_pton(AF_INET, config.publicBroadcastIp.c_str(), &mreq.imr_multiaddr);
        mreq.imr_interface.s_addr = INADDR_ANY;
        setsockopt(m_fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
        m_running.store(true);
        m_thread = std::thread(&PublicFeed::run, this);
    }
    ~PublicFeed() {
        m_running.store(false);
        if (m_thread.joinable()) m_thread.join();
        close(m_fd);
    }
    size_t count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_datagrams.size();
    }
    std::vector<std::string> datagrams() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_datagrams;
    }

private:
    void run() {
        char buffer[2048];
        while (m_running.load()) {
            const ssize_t n = recv(m_fd, buffer, sizeof(buffer), 0);
            if (n <= 0) continue;
            std::lock_guard<std::mutex> lock(m_mutex);
            m_datagrams.emplace_back(buffer, static_cast<size_t>(n));
        }
    }

    int m_fd{ -1 };
    std::atomic<bool> m_running{ false };
    std::thread m_thread;
    mutable std::mutex m_mutex;
    std::vector<std::string> m_datagrams;
};

// What EgressPublisher needs from LeaderHeartbeatClient, under the test's control.
class FakeHeartbeatClient {
public:
    FakeHeartbeatClient(ReplicaId id, bool leader, FencingEpoch epoch)
        : m_id(id), m_leader(leader), m_epoch(epoch) {
    }
    bool isLeader() const { return m_leader.load(); }
    FencingEpoch currentEpoch() const { return m_epoch.load(); }
    ReplicaId myId() const { return m_id; }
    void setLeader(bool leader) { m_leader.store(leader); }

private:
    ReplicaId m_id;
    std::atomic<bool> m_leader;
    std::atomic<FencingEpoch> m_epoch;
};

// The only thing LeaderHeartbeatClient asks of a matching service.
struct StubMatcher {
    std::atomic<uint64_t> applied{ 0 };
    uint64_t lastAppliedSeq() const { return applied.load(); }
};

// --- helpers -----------------------------------------------------------------------------------

NetworkConfig MakeEgressConfig(const char* egressGroup, uint16_t egressPort,
    const char* publicGroup, uint16_t publicPort, uint16_t arbiterPort) {
    NetworkConfig config;
    config.egressMulticastIp = egressGroup;
    config.egressMulticastPort = egressPort;
    config.publicBroadcastIp = publicGroup;
    config.publicBroadcastPort = publicPort;
    config.arbitrationPort = arbiterPort;
    config.retransmitServerIp = "127.0.0.1"; // where LeaderHeartbeatClient finds the arbiter
    return config;
}

template <typename Predicate>
bool WaitUntil(Predicate done, std::chrono::milliseconds timeout = std::chrono::seconds(3)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (done()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return done();
}

void SendBytes(const NetworkConfig& config, const void* bytes, size_t length) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    ASSERT_GE(fd, 0);
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(config.egressMulticastPort);
    inet_pton(AF_INET, config.egressMulticastIp.c_str(), &dest.sin_addr);
    ASSERT_EQ(static_cast<ssize_t>(length), sendto(fd, bytes, length, 0, (sockaddr*)&dest, sizeof(dest)));
    close(fd);
}

// A message as a replica would send it, but with the epoch and sender chosen by the test.
void SendFenced(const NetworkConfig& config, FencingEpoch epoch, ReplicaId replica, const TestEvent& event) {
    const FencedMessage<TestEvent> msg{ epoch, replica, event };
    SendBytes(config, &msg, sizeof(msg));
}

// The gateway side: the arbiter whose epoch fences, the recording gateway, and a public feed
// subscriber, with EgressGateway running. The arbiter is built but not started; a test starts it
// when it needs a real epoch.
struct GatewayRig {
    GatewayRig(const NetworkConfig& cfg, uint16_t arbiterPort,
        std::chrono::milliseconds staleAfter = std::chrono::milliseconds(50))
        : config(cfg), arbiter(arbiterPort, staleAfter), feed(config), egress(gateway, config, arbiter) {
        egress.start();
        std::this_thread::sleep_for(GROUP_JOIN_WAIT);
    }
    ~GatewayRig() {
        egress.stop();
        arbiter.stop();
    }

    NetworkConfig config;
    ReplicaArbiter arbiter;
    RecordingGateway gateway;
    PublicFeed feed;
    EgressGateway<RecordingGateway, TestEvent, TestEncoder> egress;
};

void PushWhenThereIsRoom(SPSCQueue<TestEvent>& queue, const TestEvent& event) {
    while (!queue.try_push(event)) std::this_thread::yield();
}

} // namespace

// EgressGateway prints every event it takes. Send that text to a buffer for the length of a test,
// so it neither floods the test log nor hides a failure message (gtest writes with printf, not cout).
class EgressTest : public ::testing::Test {
protected:
    void SetUp() override { m_previous = std::cout.rdbuf(m_printed.rdbuf()); }
    void TearDown() override { std::cout.rdbuf(m_previous); }
    // Read it only after EgressGateway has been stopped, so its thread is no longer writing.
    std::string Printed() const { return m_printed.str(); }

private:
    std::ostringstream m_printed;
    std::streambuf* m_previous = nullptr;
};

// =====================================================================
// Publisher and gateway
// =====================================================================

TEST_F(EgressTest, LeaderPublishesAndTheGatewayRoutesEachEventToItsSessionAndTheFeed) {
    GatewayRig rig(MakeEgressConfig("239.255.2.1", 26301, "239.255.3.1", 27301, 46301), 46301);
    rig.gateway.connect(7);
    SPSCQueue<TestEvent> queue;
    FakeHeartbeatClient leader(1, true, 1);
    EgressPublisher<TestEvent, 1024, FakeHeartbeatClient> publisher(queue, rig.config, leader);
    publisher.start();

    queue.try_push(TestEvent{ 7, 100 });
    queue.try_push(TestEvent{ 7, 101 });
    queue.try_push(TestEvent{ 8, 102 }); // session 8 is not connected

    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 3 && rig.feed.count() >= 3; }));
    publisher.stop();
    rig.egress.stop();

    const std::vector<Sent> sent = rig.gateway.sent();
    ASSERT_EQ(3u, sent.size());
    for (size_t i = 0; i < 3; ++i) EXPECT_EQ(100u + i, FrameValue(sent[i].frame));
    EXPECT_EQ(7u, sent[0].session);
    EXPECT_EQ(7u, sent[1].session);
    EXPECT_EQ(8u, sent[2].session);
    EXPECT_TRUE(sent[0].delivered);
    EXPECT_FALSE(sent[2].delivered) << "a session that is gone must be a quiet no-op, not an error";

    const std::vector<std::string> feed = rig.feed.datagrams();
    ASSERT_EQ(3u, feed.size()) << "the public feed gets every event, whatever its session";
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(sent[i].frame, feed[i]) << "the feed carries the same bytes the client was sent";
    }

    // One line of console output per event, from the encoder's print().
    const std::string printed = Printed();
    size_t lines = 0;
    for (size_t at = printed.find("[egress] "); at != std::string::npos; at = printed.find("[egress] ", at + 1)) ++lines;
    EXPECT_EQ(3u, lines);
}

TEST_F(EgressTest, EventsKeepTheirOrderThroughThePublisherAndTheGateway) {
    constexpr uint64_t EVENTS = 1000;
    constexpr uint64_t CHUNK = 100; // egress datagrams are not recovered, so stay well inside the socket buffer
    GatewayRig rig(MakeEgressConfig("239.255.2.2", 26302, "239.255.3.2", 27302, 46302), 46302);
    rig.gateway.connect(1);
    SPSCQueue<TestEvent> queue;
    FakeHeartbeatClient leader(1, true, 1);
    EgressPublisher<TestEvent, 1024, FakeHeartbeatClient> publisher(queue, rig.config, leader);
    publisher.start();

    for (uint64_t base = 0; base < EVENTS; base += CHUNK) {
        for (uint64_t i = 0; i < CHUNK; ++i) PushWhenThereIsRoom(queue, TestEvent{ 1, base + i });
        ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= base + CHUNK; }));
    }
    ASSERT_TRUE(WaitUntil([&] { return rig.feed.count() >= EVENTS; }));
    publisher.stop();
    rig.egress.stop();

    const std::vector<Sent> sent = rig.gateway.sent();
    const std::vector<std::string> feed = rig.feed.datagrams();
    ASSERT_EQ(EVENTS, sent.size());
    ASSERT_EQ(EVENTS, feed.size());
    for (uint64_t i = 0; i < EVENTS; ++i) {
        if (FrameValue(sent[i].frame) != i || FrameValue(feed[i]) != i) {
            ADD_FAILURE() << "first out-of-order or repeated event at position " << i;
            break;
        }
    }
}

TEST_F(EgressTest, ASessionThatHasGoneDoesNotStopTheFeed) {
    GatewayRig rig(MakeEgressConfig("239.255.2.3", 26303, "239.255.3.3", 27303, 46303), 46303);
    SPSCQueue<TestEvent> queue; // nobody is connected
    FakeHeartbeatClient leader(1, true, 1);
    EgressPublisher<TestEvent, 1024, FakeHeartbeatClient> publisher(queue, rig.config, leader);
    publisher.start();

    for (uint64_t i = 0; i < 20; ++i) queue.try_push(TestEvent{ static_cast<SessionId>(50 + i), i });

    ASSERT_TRUE(WaitUntil([&] { return rig.feed.count() >= 20; }));
    publisher.stop();
    rig.egress.stop();
    EXPECT_EQ(20u, rig.gateway.sentCount());
    for (const Sent& s : rig.gateway.sent()) EXPECT_FALSE(s.delivered);
}

TEST_F(EgressTest, FollowerDrainsItsQueueButSendsNothing) {
    GatewayRig rig(MakeEgressConfig("239.255.2.4", 26304, "239.255.3.4", 27304, 46304), 46304);
    SPSCQueue<TestEvent> queue;
    FakeHeartbeatClient follower(2, false, 1);
    EgressPublisher<TestEvent, 1024, FakeHeartbeatClient> publisher(queue, rig.config, follower);
    publisher.start();

    for (uint64_t i = 0; i < 200; ++i) queue.try_push(TestEvent{ 1, i });

    // The matching thread must never back up behind a replica that is not publishing.
    ASSERT_TRUE(WaitUntil([&] { return queue.empty(); })) << "a follower must keep draining its queue";
    std::this_thread::sleep_for(NOTHING_ARRIVES_WAIT);
    EXPECT_EQ(0u, rig.gateway.sentCount());
    EXPECT_EQ(0u, rig.feed.count());
    publisher.stop();
}

TEST_F(EgressTest, ReplicaStartsPublishingWhenItBecomesLeaderButNotWhatItDiscardedBefore) {
    GatewayRig rig(MakeEgressConfig("239.255.2.5", 26305, "239.255.3.5", 27305, 46305), 46305);
    rig.gateway.connect(1);
    SPSCQueue<TestEvent> queue;
    FakeHeartbeatClient replica(2, false, 1);
    EgressPublisher<TestEvent, 1024, FakeHeartbeatClient> publisher(queue, rig.config, replica);
    publisher.start();

    for (uint64_t i = 0; i < 10; ++i) queue.try_push(TestEvent{ 1, i }); // produced as a follower
    ASSERT_TRUE(WaitUntil([&] { return queue.empty(); }));

    replica.setLeader(true);
    for (uint64_t i = 100; i < 110; ++i) queue.try_push(TestEvent{ 1, i });

    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 10; }));
    std::this_thread::sleep_for(NOTHING_ARRIVES_WAIT);
    publisher.stop();
    rig.egress.stop();

    const std::vector<Sent> sent = rig.gateway.sent();
    ASSERT_EQ(10u, sent.size()) << "events produced while a follower are gone: egress has no gap-fill";
    for (size_t i = 0; i < 10; ++i) EXPECT_EQ(100u + i, FrameValue(sent[i].frame));
}

// =====================================================================
// Fencing
// =====================================================================

TEST_F(EgressTest, MessageFromAnOlderEpochIsDropped) {
    GatewayRig rig(MakeEgressConfig("239.255.2.6", 26306, "239.255.3.6", 27306, 46306), 46306);
    rig.gateway.connect(1);
    rig.arbiter.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_EQ(FencingEpoch(1), SendHeartbeat(46306, 1, 0).epoch);

    SendFenced(rig.config, 0, 9, TestEvent{ 1, 10 }); // older than the arbiter's epoch 1: dropped
    SendFenced(rig.config, 1, 1, TestEvent{ 1, 11 }); // the current epoch: delivered
    SendFenced(rig.config, 5, 1, TestEvent{ 1, 12 }); // newer than the arbiter knows: delivered (only older is stale)

    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 2; }));
    std::this_thread::sleep_for(NOTHING_ARRIVES_WAIT);
    rig.egress.stop();

    const std::vector<Sent> sent = rig.gateway.sent();
    ASSERT_EQ(2u, sent.size());
    EXPECT_EQ(11u, FrameValue(sent[0].frame));
    EXPECT_EQ(12u, FrameValue(sent[1].frame));
    EXPECT_EQ(2u, rig.feed.count()) << "a dropped message must not reach the public feed either";
}

TEST_F(EgressTest, LateMessagesFromADeposedLeaderAreDroppedAfterFailover) {
    GatewayRig rig(MakeEgressConfig("239.255.2.7", 26307, "239.255.3.7", 27307, 46307), 46307,
        std::chrono::milliseconds(50));
    rig.gateway.connect(1);
    rig.arbiter.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    const HeartbeatResponse first = SendHeartbeat(46307, 1, 0);
    ASSERT_EQ(ReplicaId(1), first.leaderId);
    SendFenced(rig.config, first.epoch, 1, TestEvent{ 1, 20 });
    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 1; }));

    // Replica 1 goes quiet. Replica 2 checks in after the stale window and takes over.
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    const HeartbeatResponse second = SendHeartbeat(46307, 2, 0);
    ASSERT_EQ(ReplicaId(2), second.leaderId);
    ASSERT_GT(second.epoch, first.epoch);

    SendFenced(rig.config, first.epoch, 1, TestEvent{ 1, 21 });  // replica 1, late, still on the old epoch
    SendFenced(rig.config, second.epoch, 2, TestEvent{ 1, 22 }); // the new leader

    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 2; }));
    std::this_thread::sleep_for(NOTHING_ARRIVES_WAIT);
    rig.egress.stop();

    const std::vector<Sent> sent = rig.gateway.sent();
    ASSERT_EQ(2u, sent.size());
    EXPECT_EQ(20u, FrameValue(sent[0].frame));
    EXPECT_EQ(22u, FrameValue(sent[1].frame)) << "the deposed leader's late message must not get through";
}

TEST_F(EgressTest, ShortDatagramIsIgnored) {
    GatewayRig rig(MakeEgressConfig("239.255.2.8", 26308, "239.255.3.8", 27308, 46308), 46308);
    rig.gateway.connect(1);

    const char junk[5] = { 1, 2, 3, 4, 5 };
    SendBytes(rig.config, junk, sizeof(junk));
    SendFenced(rig.config, 1, 1, TestEvent{ 1, 30 });

    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 1; }));
    std::this_thread::sleep_for(NOTHING_ARRIVES_WAIT);
    rig.egress.stop();
    ASSERT_EQ(1u, rig.gateway.sentCount());
    EXPECT_EQ(30u, FrameValue(rig.gateway.sent()[0].frame));
}

TEST_F(EgressTest, StopReturnsPromptlyWhenNothingArrives) {
    GatewayRig rig(MakeEgressConfig("239.255.2.9", 26309, "239.255.3.9", 27309, 46309), 46309);
    const auto started = std::chrono::steady_clock::now();
    rig.egress.stop();
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(1));
}

// =====================================================================
// The whole chain, with real heartbeats
// =====================================================================

TEST_F(EgressTest, ASurvivingReplicaTakesOverPublishingWhenTheLeaderDies) {
    const NetworkConfig config = MakeEgressConfig("239.255.2.10", 26310, "239.255.3.10", 27310, 46310);
    GatewayRig rig(config, 46310, std::chrono::milliseconds(50));
    rig.gateway.connect(1);
    rig.arbiter.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    StubMatcher matcherA, matcherB;
    LeaderHeartbeatClient<StubMatcher> clientA(1, matcherA, config), clientB(2, matcherB, config);
    SPSCQueue<TestEvent> queueA, queueB;
    EgressPublisher<TestEvent, 1024, LeaderHeartbeatClient<StubMatcher>> publisherA(queueA, config, clientA);
    EgressPublisher<TestEvent, 1024, LeaderHeartbeatClient<StubMatcher>> publisherB(queueB, config, clientB);

    // Replica A is first to check in, so it leads. Replica B joins as a follower.
    clientA.start();
    ASSERT_TRUE(WaitUntil([&] { return clientA.isLeader(); }));
    clientB.start();
    publisherA.start();
    publisherB.start();
    const FencingEpoch epochBeforeFailover = clientA.currentEpoch();

    // Both replicas produce the same events, as deterministic replicas do. Only the leader publishes them.
    constexpr uint64_t FIRST_BATCH = 50;
    for (uint64_t i = 0; i < FIRST_BATCH; ++i) {
        PushWhenThereIsRoom(queueA, TestEvent{ 1, i });
        PushWhenThereIsRoom(queueB, TestEvent{ 1, i });
    }
    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= FIRST_BATCH; }));
    ASSERT_TRUE(WaitUntil([&] { return queueA.empty() && queueB.empty(); }));
    std::this_thread::sleep_for(NOTHING_ARRIVES_WAIT);
    ASSERT_EQ(FIRST_BATCH, rig.gateway.sentCount()) << "the follower must not publish a second copy";

    // Replica A dies: its publisher and its heartbeats stop together.
    publisherA.stop();
    clientA.stop();
    ASSERT_TRUE(WaitUntil([&] { return clientB.isLeader(); }, std::chrono::seconds(5)))
        << "the survivor was not told it leads";
    EXPECT_GT(clientB.currentEpoch(), epochBeforeFailover);

    // A message from the dead leader, still on the old epoch, must not get through.
    SendFenced(config, epochBeforeFailover, 1, TestEvent{ 1, 999 });

    constexpr uint64_t SECOND_BATCH = 50;
    for (uint64_t i = FIRST_BATCH; i < FIRST_BATCH + SECOND_BATCH; ++i) PushWhenThereIsRoom(queueB, TestEvent{ 1, i });
    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= FIRST_BATCH + SECOND_BATCH; }));
    std::this_thread::sleep_for(NOTHING_ARRIVES_WAIT);

    publisherB.stop();
    clientB.stop();
    rig.egress.stop();

    const std::vector<Sent> sent = rig.gateway.sent();
    ASSERT_EQ(FIRST_BATCH + SECOND_BATCH, sent.size());
    for (uint64_t i = 0; i < sent.size(); ++i) {
        if (FrameValue(sent[i].frame) != i) {
            ADD_FAILURE() << "event " << i << " is missing, repeated or out of order (got "
                << FrameValue(sent[i].frame) << ")";
            break;
        }
    }
}
