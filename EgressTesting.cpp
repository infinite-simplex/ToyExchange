// Egress: how a result gets from a replica to a client and to the public feed.
//
// Core has no event type or wire encoding of its own, so these tests use a minimal stand-in
// event (TestEvent) and encoder (TestEncoder), and a gateway that records what it is asked to
// send (RecordingGateway). Everything else is real: EgressPublisher, EgressGateway,
// ReplicaArbiter, LeaderHeartbeatClient and the multicast between them.
//
// What is checked:
//  - only the leader publishes, and a follower still drains its queue;
//  - the publisher sends what its queue holds as one datagram and never waits to fill one;
//  - the gateway sends each client its frames for a batch in ONE call, in order, packs the public feed
//    into few datagrams, and prints one console line per event (or none, when told not to);
//  - a datagram from an older epoch is dropped (fencing), and malformed ones are counted and ignored;
//  - events that never arrive are counted from the ordinals, because replicas number every event, and
//    reported with a warning that is rate limited;
//  - after the leader dies the survivor takes over while the dead leader's late datagrams are dropped.
//
// What is not checked, because the code does not do it: egress has no gap-fill. A test that
// loses datagrams would only prove they stay lost.

#include <gtest/gtest.h>
#include "ArbiterTestSupport.hpp"
#include "EgressBatch.hpp"
#include "EgressGateway.hpp"
#include "EgressPublisher.hpp"
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
constexpr size_t FRAME_BYTES = TestEncoder::FRAME_BYTES;

uint64_t FrameValue(const std::string& frame) {
    uint64_t value = 0;
    std::memcpy(&value, frame.data(), sizeof(value));
    return value;
}

std::vector<std::string> SplitFrames(const std::string& bytes) {
    std::vector<std::string> frames;
    for (size_t at = 0; at + FRAME_BYTES <= bytes.size(); at += FRAME_BYTES) frames.push_back(bytes.substr(at, FRAME_BYTES));
    return frames;
}

// One frame, as the gateway was asked to send it.
struct Sent {
    SessionId session;
    std::string frame;
    bool delivered; // the session was connected
};

// One call to sendToSession.
struct Call {
    SessionId session;
    std::string bytes;
    bool delivered;
};

// Stands in for NetworkGateway on the egress side: only sendToSession matters here.
class RecordingGateway {
public:
    bool sendToSession(SessionId id, const char* data, size_t len) {
        std::lock_guard<std::mutex> lock(m_mutex);
        const bool connected = m_connected.count(id) != 0;
        m_calls.push_back({ id, std::string(data, len), connected });
        return connected;
    }
    // EgressGateway offers a gateway that holds output for a slow client the chance to flush it.
    void flushPendingSessions() { m_flushes.fetch_add(1); }

    void connect(SessionId id) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_connected.insert(id);
    }
    std::vector<Call> calls() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_calls;
    }
    // Every frame, in the order sent, whichever call carried it.
    std::vector<Sent> sent() const {
        std::vector<Sent> frames;
        for (const Call& call : calls()) {
            for (const std::string& frame : SplitFrames(call.bytes)) frames.push_back({ call.session, frame, call.delivered });
        }
        return frames;
    }
    size_t sentCount() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        size_t frames = 0;
        for (const Call& call : m_calls) frames += call.bytes.size() / FRAME_BYTES;
        return frames;
    }
    uint64_t flushes() const { return m_flushes.load(); }

private:
    mutable std::mutex m_mutex;
    std::vector<Call> m_calls;
    std::set<SessionId> m_connected;
    std::atomic<uint64_t> m_flushes{ 0 };
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
    size_t datagramCount() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_datagrams.size();
    }
    std::vector<std::string> datagrams() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_datagrams;
    }
    // Every frame, in order, however the datagrams packed them.
    std::vector<std::string> frames() const {
        std::vector<std::string> all;
        for (const std::string& datagram : datagrams()) {
            for (const std::string& frame : SplitFrames(datagram)) all.push_back(frame);
        }
        return all;
    }
    size_t frameCount() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        size_t frames = 0;
        for (const std::string& datagram : m_datagrams) frames += datagram.size() / FRAME_BYTES;
        return frames;
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

using FakePublisher = EgressPublisher<TestEvent, 1024, FakeHeartbeatClient>;

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

// A datagram as a replica would send it, but with the epoch, sender and ordinal chosen by the test.
void SendBatch(const NetworkConfig& config, FencingEpoch epoch, ReplicaId replica, uint64_t firstOrdinal,
    const std::vector<TestEvent>& events) {
    std::vector<char> datagram(sizeof(EgressBatchHeader) + events.size() * sizeof(TestEvent));
    const EgressBatchHeader header{ epoch, firstOrdinal, replica, static_cast<uint16_t>(events.size()), 0 };
    std::memcpy(datagram.data(), &header, sizeof(header));
    std::memcpy(datagram.data() + sizeof(header), events.data(), events.size() * sizeof(TestEvent));
    SendBytes(config, datagram.data(), datagram.size());
}

void SendFenced(const NetworkConfig& config, FencingEpoch epoch, ReplicaId replica, const TestEvent& event,
    uint64_t firstOrdinal = 0) {
    SendBatch(config, epoch, replica, firstOrdinal, { event });
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

size_t CountLines(const std::string& text, const char* prefix) {
    size_t lines = 0;
    for (size_t at = text.find(prefix); at != std::string::npos; at = text.find(prefix, at + 1)) ++lines;
    return lines;
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
// The wire format
// =====================================================================

TEST(EgressBatchTest, HowManyEventsFitInADatagram) {
    EXPECT_EQ(24u, sizeof(EgressBatchHeader));
    EXPECT_EQ((1400u - 24u) / sizeof(TestEvent), EventsPerEgressDatagram<TestEvent>(1400));
    EXPECT_EQ(1u, EventsPerEgressDatagram<TestEvent>(sizeof(EgressBatchHeader) + sizeof(TestEvent) - 1))
        << "a limit smaller than one event still lets one event out, alone";
    EXPECT_EQ(1u, EventsPerEgressDatagram<TestEvent>(0));
    EXPECT_EQ(size_t{ UINT16_MAX }, EventsPerEgressDatagram<char>(10000000)) << "the count field is 16 bits";
}

// =====================================================================
// Publisher and gateway
// =====================================================================

TEST_F(EgressTest, LeaderPublishesAndTheGatewayRoutesEachEventToItsSessionAndTheFeed) {
    GatewayRig rig(MakeEgressConfig("239.255.2.1", 26301, "239.255.3.1", 27301, 46301), 46301);
    rig.gateway.connect(7);
    SPSCQueue<TestEvent> queue;
    FakeHeartbeatClient leader(1, true, 1);
    FakePublisher publisher(queue, rig.config, leader);
    publisher.start();

    queue.try_push(TestEvent{ 7, 100 });
    queue.try_push(TestEvent{ 7, 101 });
    queue.try_push(TestEvent{ 8, 102 }); // session 8 is not connected

    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 3 && rig.feed.frameCount() >= 3; }));
    publisher.stop();
    rig.egress.stop();

    const std::vector<Sent> sent = rig.gateway.sent();
    ASSERT_EQ(3u, sent.size());
    std::vector<Sent> bySession7;
    for (const Sent& s : sent) if (s.session == 7) bySession7.push_back(s);
    ASSERT_EQ(2u, bySession7.size());
    EXPECT_EQ(100u, FrameValue(bySession7[0].frame));
    EXPECT_EQ(101u, FrameValue(bySession7[1].frame)) << "one client's frames keep their order";
    EXPECT_TRUE(bySession7[0].delivered);
    for (const Sent& s : sent) {
        if (s.session == 8) {
            EXPECT_EQ(102u, FrameValue(s.frame));
            EXPECT_FALSE(s.delivered) << "a session that is gone must be a quiet no-op, not an error";
        }
    }

    const std::vector<std::string> feed = rig.feed.frames();
    ASSERT_EQ(3u, feed.size()) << "the public feed gets every event, whatever its session";
    for (size_t i = 0; i < 3; ++i) EXPECT_EQ(100u + i, FrameValue(feed[i])) << "the feed keeps event order";

    // One line per event, from the encoder's print().
    EXPECT_EQ(3u, CountLines(Printed(), "[egress] "));
    const EgressStats stats = rig.egress.stats();
    EXPECT_EQ(3u, stats.events);
    EXPECT_EQ(0u, stats.missingEvents);
    EXPECT_EQ(0u, stats.repeatedEvents);
}

TEST_F(EgressTest, EventsKeepTheirOrderThroughThePublisherAndTheGateway) {
    constexpr uint64_t EVENTS = 1000;
    constexpr uint64_t CHUNK = 100; // egress datagrams are not recovered, so stay well inside the socket buffer
    GatewayRig rig(MakeEgressConfig("239.255.2.2", 26302, "239.255.3.2", 27302, 46302), 46302);
    rig.gateway.connect(1);
    SPSCQueue<TestEvent> queue;
    FakeHeartbeatClient leader(1, true, 1);
    FakePublisher publisher(queue, rig.config, leader);
    publisher.start();

    for (uint64_t base = 0; base < EVENTS; base += CHUNK) {
        for (uint64_t i = 0; i < CHUNK; ++i) PushWhenThereIsRoom(queue, TestEvent{ 1, base + i });
        ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= base + CHUNK; }));
    }
    ASSERT_TRUE(WaitUntil([&] { return rig.feed.frameCount() >= EVENTS; }));
    publisher.stop();
    rig.egress.stop();

    const std::vector<Sent> sent = rig.gateway.sent();
    const std::vector<std::string> feed = rig.feed.frames();
    ASSERT_EQ(EVENTS, sent.size());
    ASSERT_EQ(EVENTS, feed.size());
    for (uint64_t i = 0; i < EVENTS; ++i) {
        if (FrameValue(sent[i].frame) != i || FrameValue(feed[i]) != i) {
            ADD_FAILURE() << "first out-of-order or repeated event at position " << i;
            break;
        }
    }
    EXPECT_EQ(0u, rig.egress.stats().missingEvents) << "the ordinals must run without a gap";
}

TEST_F(EgressTest, ASessionThatHasGoneDoesNotStopTheFeed) {
    GatewayRig rig(MakeEgressConfig("239.255.2.3", 26303, "239.255.3.3", 27303, 46303), 46303);
    SPSCQueue<TestEvent> queue; // nobody is connected
    FakeHeartbeatClient leader(1, true, 1);
    FakePublisher publisher(queue, rig.config, leader);
    publisher.start();

    for (uint64_t i = 0; i < 20; ++i) queue.try_push(TestEvent{ static_cast<SessionId>(50 + i), i });

    ASSERT_TRUE(WaitUntil([&] { return rig.feed.frameCount() >= 20; }));
    publisher.stop();
    rig.egress.stop();
    EXPECT_EQ(20u, rig.gateway.sentCount());
    for (const Sent& s : rig.gateway.sent()) EXPECT_FALSE(s.delivered);
}

// =====================================================================
// Batching
// =====================================================================

TEST_F(EgressTest, AnEventThatIsAloneGoesOutAloneAndAtOnce) {
    GatewayRig rig(MakeEgressConfig("239.255.2.11", 26311, "239.255.3.11", 27311, 46311), 46311);
    rig.gateway.connect(1);
    SPSCQueue<TestEvent> queue;
    FakeHeartbeatClient leader(1, true, 1);
    FakePublisher publisher(queue, rig.config, leader);
    publisher.start();

    queue.try_push(TestEvent{ 1, 1 });
    ASSERT_TRUE(WaitUntil([&] { return rig.egress.stats().batches >= 1; }, std::chrono::milliseconds(200)))
        << "a lone event must not wait for others to fill a datagram";
    EXPECT_EQ(1u, rig.egress.stats().events);

    std::this_thread::sleep_for(std::chrono::milliseconds(50)); // a quiet spell, then another lone event
    queue.try_push(TestEvent{ 1, 2 });
    ASSERT_TRUE(WaitUntil([&] { return rig.egress.stats().batches >= 2; }, std::chrono::milliseconds(200)));
    publisher.stop();
    rig.egress.stop();
    EXPECT_EQ(2u, rig.egress.stats().batches) << "each lone event is a datagram of its own";
    EXPECT_EQ(2u, rig.gateway.calls().size());
}

TEST_F(EgressTest, EventsAlreadyWaitingShareDatagramsUpToTheLimit) {
    constexpr size_t PER_DATAGRAM = 10;
    constexpr size_t EVENTS = 100;
    NetworkConfig config = MakeEgressConfig("239.255.2.12", 26312, "239.255.3.12", 27312, 46312);
    config.egressDatagramMaxBytes = sizeof(EgressBatchHeader) + PER_DATAGRAM * sizeof(TestEvent);
    GatewayRig rig(config, 46312);
    rig.gateway.connect(1);
    SPSCQueue<TestEvent> queue;
    for (uint64_t i = 0; i < EVENTS; ++i) queue.try_push(TestEvent{ 1, i }); // all waiting before the publisher starts
    FakeHeartbeatClient leader(1, true, 1);
    FakePublisher publisher(queue, rig.config, leader);
    publisher.start();

    ASSERT_TRUE(WaitUntil([&] { return rig.egress.stats().events >= EVENTS; }));
    publisher.stop();
    rig.egress.stop();
    EXPECT_EQ(EVENTS / PER_DATAGRAM, rig.egress.stats().batches) << EVENTS << " waiting events, " << PER_DATAGRAM << " per datagram";
    EXPECT_EQ(0u, rig.egress.stats().missingEvents);
}

TEST_F(EgressTest, AClientGetsItsFramesForABatchInOneCall) {
    GatewayRig rig(MakeEgressConfig("239.255.2.13", 26313, "239.255.3.13", 27313, 46313), 46313);
    rig.gateway.connect(1);
    rig.gateway.connect(2);

    // One batch, two clients interleaved: 1, 2, 1, 2, 1.
    SendBatch(rig.config, 1, 1, 0, { {1, 10}, {2, 11}, {1, 12}, {2, 13}, {1, 14} });
    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 5; }));
    rig.egress.stop();

    const std::vector<Call> calls = rig.gateway.calls();
    ASSERT_EQ(2u, calls.size()) << "one call per client per batch, not one per event";
    EXPECT_EQ(1u, calls[0].session);
    EXPECT_EQ(2u, calls[1].session);
    const std::vector<std::string> first = SplitFrames(calls[0].bytes);
    const std::vector<std::string> second = SplitFrames(calls[1].bytes);
    ASSERT_EQ(3u, first.size());
    ASSERT_EQ(2u, second.size());
    EXPECT_EQ(10u, FrameValue(first[0]));
    EXPECT_EQ(12u, FrameValue(first[1]));
    EXPECT_EQ(14u, FrameValue(first[2]));
    EXPECT_EQ(11u, FrameValue(second[0]));
    EXPECT_EQ(13u, FrameValue(second[1]));
}

TEST_F(EgressTest, ThePublicFeedPacksFramesIntoFewDatagramsInOrder) {
    NetworkConfig config = MakeEgressConfig("239.255.2.14", 26314, "239.255.3.14", 27314, 46314);
    config.egressDatagramMaxBytes = 3 * FRAME_BYTES; // three frames per feed datagram
    GatewayRig rig(config, 46314);
    rig.gateway.connect(1);

    SendBatch(rig.config, 1, 1, 0, { {1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}, {1, 6} });
    ASSERT_TRUE(WaitUntil([&] { return rig.feed.frameCount() >= 7; }));
    rig.egress.stop();

    const std::vector<std::string> datagrams = rig.feed.datagrams();
    ASSERT_EQ(3u, datagrams.size()) << "seven frames at three per datagram";
    EXPECT_EQ(3u * FRAME_BYTES, datagrams[0].size());
    EXPECT_EQ(3u * FRAME_BYTES, datagrams[1].size());
    EXPECT_EQ(1u * FRAME_BYTES, datagrams[2].size());
    const std::vector<std::string> frames = rig.feed.frames();
    for (size_t i = 0; i < frames.size(); ++i) EXPECT_EQ(i, FrameValue(frames[i]));
}

TEST_F(EgressTest, ConsoleEchoCanBeSwitchedOffAndStillDeliversEverything) {
    NetworkConfig config = MakeEgressConfig("239.255.2.15", 26315, "239.255.3.15", 27315, 46315);
    config.egressConsoleEcho = false;
    GatewayRig rig(config, 46315);
    rig.gateway.connect(1);

    SendBatch(rig.config, 1, 1, 0, { {1, 0}, {1, 1}, {1, 2} });
    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 3 && rig.feed.frameCount() >= 3; }));
    rig.egress.stop();
    EXPECT_EQ(0u, CountLines(Printed(), "[egress] "));
}

TEST_F(EgressTest, TheGatewayAsksAClientHoldingBackOutputToFlushIt) {
    GatewayRig rig(MakeEgressConfig("239.255.2.16", 26316, "239.255.3.16", 27316, 46316), 46316);
    rig.gateway.connect(1);
    SendBatch(rig.config, 1, 1, 0, { {1, 0} });
    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.flushes() >= 1; })) << "after a batch";

    const uint64_t before = rig.gateway.flushes();
    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.flushes() >= before + 2; }, std::chrono::seconds(2)))
        << "and while it is quiet, so a slow client can catch up with no new traffic";
}

TEST_F(EgressTest, TheGatewayAppliesTheConfiguredReceiveBuffer) {
    NetworkConfig config = MakeEgressConfig("239.255.2.17", 26317, "239.255.3.17", 27317, 46317);
    config.egressReceiveBufferBytes = 64 * 1024; // far below any net.core.rmem_max, so the kernel grants it in full
    GatewayRig rig(config, 46317);
    EXPECT_EQ(64 * 1024, rig.egress.receiveBufferBytes());
    EXPECT_EQ(4 * 1024 * 1024, NetworkConfig{}.egressReceiveBufferBytes) << "the default";
}

// =====================================================================
// Leadership and ordinals
// =====================================================================

TEST_F(EgressTest, FollowerDrainsItsQueueButSendsNothing) {
    GatewayRig rig(MakeEgressConfig("239.255.2.4", 26304, "239.255.3.4", 27304, 46304), 46304);
    SPSCQueue<TestEvent> queue;
    FakeHeartbeatClient follower(2, false, 1);
    FakePublisher publisher(queue, rig.config, follower);
    publisher.start();

    for (uint64_t i = 0; i < 200; ++i) queue.try_push(TestEvent{ 1, i });

    // The matching thread must never back up behind a replica that is not publishing.
    ASSERT_TRUE(WaitUntil([&] { return publisher.eventsConsumed() >= 200; })) << "a follower must keep draining its queue";
    std::this_thread::sleep_for(NOTHING_ARRIVES_WAIT);
    EXPECT_EQ(0u, rig.gateway.sentCount());
    EXPECT_EQ(0u, rig.feed.frameCount());
    publisher.stop();
}

TEST_F(EgressTest, ReplicaStartsPublishingWhenItBecomesLeaderAndTheGatewayCountsWhatItNeverSaw) {
    GatewayRig rig(MakeEgressConfig("239.255.2.5", 26305, "239.255.3.5", 27305, 46305), 46305);
    rig.gateway.connect(1);
    SPSCQueue<TestEvent> queue;
    FakeHeartbeatClient replica(2, false, 1);
    FakePublisher publisher(queue, rig.config, replica);
    publisher.start();

    for (uint64_t i = 0; i < 10; ++i) queue.try_push(TestEvent{ 1, i }); // produced as a follower
    ASSERT_TRUE(WaitUntil([&] { return publisher.eventsConsumed() >= 10; }));

    replica.setLeader(true);
    for (uint64_t i = 100; i < 110; ++i) queue.try_push(TestEvent{ 1, i });

    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 10; }));
    std::this_thread::sleep_for(NOTHING_ARRIVES_WAIT);
    publisher.stop();
    rig.egress.stop();

    const std::vector<Sent> sent = rig.gateway.sent();
    ASSERT_EQ(10u, sent.size()) << "events produced while a follower are gone: egress has no gap-fill";
    for (size_t i = 0; i < 10; ++i) EXPECT_EQ(100u + i, FrameValue(sent[i].frame));
    EXPECT_EQ(10u, rig.egress.stats().missingEvents)
        << "the first ten events were numbered but never published, and the gateway can tell";
}

TEST_F(EgressTest, AnOrdinalThatArrivesAgainIsCounted) {
    GatewayRig rig(MakeEgressConfig("239.255.2.18", 26318, "239.255.3.18", 27318, 46318), 46318);
    rig.gateway.connect(1);
    SendBatch(rig.config, 1, 1, 0, { {1, 0}, {1, 1}, {1, 2} });
    ASSERT_TRUE(WaitUntil([&] { return rig.egress.stats().batches >= 1; }));
    SendBatch(rig.config, 1, 2, 1, { {1, 1}, {1, 2}, {1, 3} }); // another replica's copy: ordinals 1 and 2 are repeats
    ASSERT_TRUE(WaitUntil([&] { return rig.egress.stats().batches >= 2; }));
    rig.egress.stop();

    const EgressStats stats = rig.egress.stats();
    EXPECT_EQ(2u, stats.repeatedEvents);
    EXPECT_EQ(0u, stats.missingEvents);
}

TEST_F(EgressTest, EventsThatNeverArrivedAreReportedOnceAndNotOncePerDatagram) {
    GatewayRig rig(MakeEgressConfig("239.255.2.19", 26319, "239.255.3.19", 27319, 46319), 46319);
    rig.gateway.connect(1);

    testing::internal::CaptureStderr();
    SendBatch(rig.config, 1, 1, 0, { {1, 0} });
    SendBatch(rig.config, 1, 1, 5, { {1, 5} });  // ordinals 1 to 4 never arrived: reported at once
    SendBatch(rig.config, 1, 1, 9, { {1, 9} });  // 6 to 8: inside the report interval, so held back
    SendBatch(rig.config, 1, 1, 12, { {1, 12} }); // 10 and 11: held back as well
    ASSERT_TRUE(WaitUntil([&] { return rig.egress.stats().batches >= 4; }));
    rig.egress.stop();                            // the last report names what was held back
    const std::string log = testing::internal::GetCapturedStderr();

    EXPECT_NE(std::string::npos, log.find("did not receive 4 events (4 since start)"));
    EXPECT_NE(std::string::npos, log.find("did not receive 5 events (9 since start)"))
        << "what was held back must still be reported, at the latest when the gateway stops";
    size_t reports = 0;
    for (size_t at = log.find("[WARNING] Egress gateway did not receive"); at != std::string::npos;
         at = log.find("[WARNING] Egress gateway did not receive", at + 1)) {
        ++reports;
    }
    EXPECT_EQ(2u, reports) << "three gaps in a burst must cost two lines, not three";
}

// =====================================================================
// Fencing and bad datagrams
// =====================================================================

TEST_F(EgressTest, DatagramFromAnOlderEpochIsDropped) {
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
    EXPECT_EQ(2u, rig.feed.frameCount()) << "a dropped datagram must not reach the public feed either";
    EXPECT_EQ(1u, rig.egress.stats().staleBatches);
    EXPECT_EQ(1u, rig.egress.stats().staleEvents);
}

TEST_F(EgressTest, LateDatagramsFromADeposedLeaderAreDroppedAfterFailover) {
    GatewayRig rig(MakeEgressConfig("239.255.2.7", 26307, "239.255.3.7", 27307, 46307), 46307,
        std::chrono::milliseconds(50));
    rig.gateway.connect(1);
    rig.arbiter.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    const HeartbeatResponse first = SendHeartbeat(46307, 1, 0);
    ASSERT_EQ(ReplicaId(1), first.leaderId);
    SendFenced(rig.config, first.epoch, 1, TestEvent{ 1, 20 }, 0);
    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 1; }));

    // Replica 1 goes quiet. Replica 2 checks in after the stale window and takes over.
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    const HeartbeatResponse second = SendHeartbeat(46307, 2, 0);
    ASSERT_EQ(ReplicaId(2), second.leaderId);
    ASSERT_GT(second.epoch, first.epoch);

    SendFenced(rig.config, first.epoch, 1, TestEvent{ 1, 21 }, 1);  // replica 1, late, still on the old epoch
    SendFenced(rig.config, second.epoch, 2, TestEvent{ 1, 22 }, 1); // the new leader

    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 2; }));
    std::this_thread::sleep_for(NOTHING_ARRIVES_WAIT);
    rig.egress.stop();

    const std::vector<Sent> sent = rig.gateway.sent();
    ASSERT_EQ(2u, sent.size());
    EXPECT_EQ(20u, FrameValue(sent[0].frame));
    EXPECT_EQ(22u, FrameValue(sent[1].frame)) << "the deposed leader's late datagram must not get through";
}

TEST_F(EgressTest, MalformedDatagramsAreCountedAndIgnored) {
    GatewayRig rig(MakeEgressConfig("239.255.2.8", 26308, "239.255.3.8", 27308, 46308), 46308);
    rig.gateway.connect(1);

    const char junk[5] = { 1, 2, 3, 4, 5 }; // shorter than a header
    SendBytes(rig.config, junk, sizeof(junk));

    // A header that promises three events, followed by room for one.
    std::vector<char> torn(sizeof(EgressBatchHeader) + sizeof(TestEvent), 0);
    const EgressBatchHeader lying{ 1, 0, 1, 3, 0 };
    std::memcpy(torn.data(), &lying, sizeof(lying));
    SendBytes(rig.config, torn.data(), torn.size());

    // A header with a count of zero.
    std::vector<char> empty(sizeof(EgressBatchHeader), 0);
    const EgressBatchHeader none{ 1, 0, 1, 0, 0 };
    std::memcpy(empty.data(), &none, sizeof(none));
    SendBytes(rig.config, empty.data(), empty.size());

    SendFenced(rig.config, 1, 1, TestEvent{ 1, 30 });

    ASSERT_TRUE(WaitUntil([&] { return rig.gateway.sentCount() >= 1; }));
    std::this_thread::sleep_for(NOTHING_ARRIVES_WAIT);
    rig.egress.stop();
    ASSERT_EQ(1u, rig.gateway.sentCount());
    EXPECT_EQ(30u, FrameValue(rig.gateway.sent()[0].frame));
    EXPECT_EQ(3u, rig.egress.stats().malformedDatagrams);
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
    ASSERT_TRUE(WaitUntil([&] { return publisherA.eventsConsumed() >= FIRST_BATCH && publisherB.eventsConsumed() >= FIRST_BATCH; }));
    std::this_thread::sleep_for(NOTHING_ARRIVES_WAIT);
    ASSERT_EQ(FIRST_BATCH, rig.gateway.sentCount()) << "the follower must not publish a second copy";

    // Replica A dies: its publisher and its heartbeats stop together.
    publisherA.stop();
    clientA.stop();
    ASSERT_TRUE(WaitUntil([&] { return clientB.isLeader(); }, std::chrono::seconds(5)))
        << "the survivor was not told it leads";
    EXPECT_GT(clientB.currentEpoch(), epochBeforeFailover);

    // A datagram from the dead leader, still on the old epoch, must not get through.
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

    // Both replicas numbered their events the same way, so the survivor's first datagram carried
    // exactly the next ordinal: nothing is missing and nothing repeats.
    const EgressStats stats = rig.egress.stats();
    EXPECT_EQ(0u, stats.missingEvents);
    EXPECT_EQ(0u, stats.repeatedEvents);
    EXPECT_EQ(1u, stats.staleBatches);
}
