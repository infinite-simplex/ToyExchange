// Ingress recovery: what happens when a replica joins late, restarts, loses
// the tail of a burst, loses part of a retransmit reply, or is sent the same
// datagram twice.
//
// Core has no protocol or product command type of its own, so these tests use
// a minimal stand-in (TestCommand) with the real SequenceStore,
// RetransmitServer and MulticastIngressReceiver. The gateway is not needed: it
// does two things per command, append to the store and then multicast, and the
// tests do those two steps by hand, always in that order.
//
// A receiver finds out it is behind in two ways: a live datagram whose
// sequence number skips ahead (the "lazy" path), or a full receive timeout
// with nothing arriving (the idle probe, NetworkConfig::ingressIdleProbe).
// Tests that target the lazy path switch the probe off so they stay
// deterministic; the probe tests leave it on and send no live datagram.

#include <gtest/gtest.h>
#include "MulticastIngressReceiver.hpp"
#include "RetransmitServer.hpp"
#include "SequenceStore.hpp"
#include "SequencedInboundMessage.hpp"
#include "RetransmitMessage.hpp"
#include "NetworkConfig.hpp"
#include "SPSCQueue.hpp"
#include "Alias.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace {

struct TestCommand {
    TraceId trace_id;  // the only field MulticastIngressReceiver reads
    uint64_t payload;  // derived from the sequence number, so order can be checked
};
using TestMessage = SequencedInboundMessage<TestCommand>;

constexpr auto GROUP_JOIN_WAIT = std::chrono::milliseconds(100);
constexpr auto NOTHING_ARRIVES_WAIT = std::chrono::milliseconds(300);
constexpr auto TWO_IDLE_TIMEOUTS = std::chrono::milliseconds(700); // receiver idle timeout is 200 ms
constexpr uint32_t RETRANSMIT_BATCH = 500; // must match RetransmitServer::MAX_BATCH

uint64_t PayloadFor(uint64_t seq) { return seq * 7 + 3; }

TestMessage MakeMessage(uint64_t seq) {
    TestMessage msg{};
    msg.sequenceNumber = seq;
    msg.ingressTaiNs = 1;
    msg.clientSessionId = 1;
    msg.command.trace_id = static_cast<TraceId>(seq);
    msg.command.payload = PayloadFor(seq);
    return msg;
}

NetworkConfig MakeConfig(const char* group, uint16_t multicastPort, uint16_t retransmitPort, bool idleProbe) {
    NetworkConfig config;
    config.multicastIp = group;
    config.multicastPort = multicastPort;
    config.retransmitServerIp = "127.0.0.1";
    config.retransmitServerPort = retransmitPort;
    config.ingressIdleProbe = idleProbe;
    return config;
}

// Stands in for NetworkGateway's fan-out: one multicast datagram. Call it only
// after the message is already in the store, as the gateway does.
void SendLive(const NetworkConfig& config, const TestMessage& msg) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    ASSERT_GE(fd, 0);
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(config.multicastPort);
    inet_pton(AF_INET, config.multicastIp.c_str(), &dest.sin_addr);
    ASSERT_EQ(static_cast<ssize_t>(sizeof(msg)),
        sendto(fd, &msg, sizeof(msg), 0, (sockaddr*)&dest, sizeof(dest)));
    close(fd);
}

bool PopWithin(SPSCQueue<TestCommand>& queue, TestCommand& out, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (queue.try_pop(out)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

// Pops until `count` commands have arrived, or until nothing new has arrived
// for `idleTimeout`. The queue holds fewer commands than a long replay, so the
// test thread has to drain it while the receiver is still filling it.
std::vector<TestCommand> Drain(SPSCQueue<TestCommand>& queue, size_t count,
    std::chrono::milliseconds idleTimeout = std::chrono::seconds(3)) {
    std::vector<TestCommand> got;
    got.reserve(count);
    auto lastProgress = std::chrono::steady_clock::now();
    while (got.size() < count && std::chrono::steady_clock::now() - lastProgress < idleTimeout) {
        TestCommand cmd{};
        if (queue.try_pop(cmd)) {
            got.push_back(cmd);
            lastProgress = std::chrono::steady_clock::now();
        }
        else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    return got;
}

// `got` must be exactly the commands for sequence numbers first .. first+count-1, in order.
void ExpectInOrder(const std::vector<TestCommand>& got, uint64_t count, uint64_t first = 0) {
    ASSERT_EQ(count, got.size()) << "wrong number of commands delivered";
    for (uint64_t i = 0; i < count; ++i) {
        if (got[i].payload != PayloadFor(first + i)) {
            ADD_FAILURE() << "first out-of-order or duplicated command at position " << i
                << ": expected payload " << PayloadFor(first + i) << ", got " << got[i].payload;
            return;
        }
    }
}

bool WaitForUnrecoverableGap(const MulticastIngressReceiver<TestCommand>& receiver) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!receiver.hitUnrecoverableGap() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return receiver.hitUnrecoverableGap();
}

// A retransmit server whose FIRST reply is cut short, as if the rest of its
// datagrams were dropped on the wire. Every later request is answered in full,
// from whatever start sequence and count the request asks for, like the real
// server does.
class LossyRetransmitServer {
public:
    LossyRetransmitServer(SequenceStore<TestCommand>& store, uint16_t port, uint32_t firstReplyRecords)
        : m_store(store), m_port(port), m_firstReplyRecords(firstReplyRecords) {
    }
    ~LossyRetransmitServer() { stop(); }

    void start() {
        m_fd = socket(AF_INET, SOCK_DGRAM, 0);
        int opt = 1;
        setsockopt(m_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        timeval timeout{};
        timeout.tv_usec = 50'000; // wake up regularly so stop() can join
        setsockopt(m_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(m_port);
        bind(m_fd, (sockaddr*)&addr, sizeof(addr));
        m_running.store(true);
        m_thread = std::thread(&LossyRetransmitServer::serve, this);
    }

    void stop() {
        if (!m_running.exchange(false)) return;
        if (m_thread.joinable()) m_thread.join();
        close(m_fd);
    }

private:
    void serve() {
        while (m_running.load()) {
            RetransmitRequest req{};
            sockaddr_in from{};
            socklen_t fromLen = sizeof(from);
            ssize_t n = recvfrom(m_fd, &req, sizeof(req), 0, (sockaddr*)&from, &fromLen);
            if (n != static_cast<ssize_t>(sizeof(req))) continue;

            uint32_t reply = std::min<uint32_t>(req.count, RETRANSMIT_BATCH);
            if (m_requests.fetch_add(1) == 0) reply = std::min(reply, m_firstReplyRecords);

            for (uint32_t i = 0; i < reply; ++i) {
                TestMessage record{};
                if (m_store.read(req.startSeq + i, record) != SequenceStore<TestCommand>::ReadResult::OK) break;
                RetransmitRecordHeader hdr{ RetransmitStatus::OK, {}, req.startSeq + i };
                char packet[sizeof(hdr) + sizeof(record)];
                std::memcpy(packet, &hdr, sizeof(hdr));
                std::memcpy(packet + sizeof(hdr), &record, sizeof(record));
                sendto(m_fd, packet, sizeof(packet), 0, (sockaddr*)&from, fromLen);
            }
        }
    }

    SequenceStore<TestCommand>& m_store;
    uint16_t m_port;
    uint32_t m_firstReplyRecords;
    int m_fd{ -1 };
    std::atomic<bool> m_running{ false };
    std::atomic<uint32_t> m_requests{ 0 };
    std::thread m_thread;
};

} // namespace

// =====================================================================
// Late join and restart, found out by a live datagram (idle probe off)
// =====================================================================

TEST(IngressRecoveryTest, LateJoinerReplaysFullHistoryBeforeTheLiveMessage) {
    constexpr uint64_t HISTORY = 2'000; // four retransmit batches
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_late_join.dat", 4096);
    for (uint64_t seq = 0; seq < HISTORY; ++seq) store.append(MakeMessage(seq)); // sequenced before any replica existed

    RetransmitServer<TestCommand> server(store, 45301);
    server.start();

    const NetworkConfig config = MakeConfig("239.255.1.1", 25201, 45301, false);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    const TestMessage live = MakeMessage(HISTORY);
    store.append(live); // the gateway appends before it multicasts
    SendLive(config, live);

    ExpectInOrder(Drain(queue, HISTORY + 1), HISTORY + 1);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT)) << "a duplicate command followed the replay";
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    receiver.stop();
    server.stop();
}

TEST(IngressRecoveryTest, RestartedReplicaReplaysTheSameStreamFromSequenceZero) {
    constexpr uint64_t FIRST_RUN = 100;     // delivered live to the first incarnation
    constexpr uint64_t WHILE_DOWN = 50;     // sequenced while no replica was running
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_restart.dat", 1024);
    RetransmitServer<TestCommand> server(store, 45302);
    server.start();
    const NetworkConfig config = MakeConfig("239.255.1.2", 25202, 45302, false);

    {
        SPSCQueue<TestCommand> firstQueue;
        MulticastIngressReceiver<TestCommand> first(firstQueue, config);
        first.start();
        std::this_thread::sleep_for(GROUP_JOIN_WAIT);
        for (uint64_t seq = 0; seq < FIRST_RUN; ++seq) {
            const TestMessage msg = MakeMessage(seq);
            store.append(msg);
            SendLive(config, msg);
        }
        ExpectInOrder(Drain(firstQueue, FIRST_RUN), FIRST_RUN);
        first.stop();
    }

    for (uint64_t seq = FIRST_RUN; seq < FIRST_RUN + WHILE_DOWN; ++seq) store.append(MakeMessage(seq));

    // A restarted process starts from nothing and expects sequence 0 again.
    // Both streams match the one canonical stream, so they match each other.
    SPSCQueue<TestCommand> secondQueue;
    MulticastIngressReceiver<TestCommand> second(secondQueue, config);
    second.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    const uint64_t liveSeq = FIRST_RUN + WHILE_DOWN;
    const TestMessage live = MakeMessage(liveSeq);
    store.append(live);
    SendLive(config, live);
    ExpectInOrder(Drain(secondQueue, liveSeq + 1), liveSeq + 1);

    // After the replay it follows live traffic without another gap.
    const TestMessage next = MakeMessage(liveSeq + 1);
    store.append(next);
    SendLive(config, next);
    const auto afterReplay = Drain(secondQueue, 1);
    ASSERT_EQ(size_t(1), afterReplay.size());
    EXPECT_EQ(PayloadFor(liveSeq + 1), afterReplay[0].payload);
    EXPECT_FALSE(second.hitUnrecoverableGap());

    second.stop();
    server.stop();
}

// =====================================================================
// Ring boundary: how far back a joiner can still reach (idle probe off)
// =====================================================================
// The store keeps the newest CAPACITY sequence numbers. A joiner needs
// everything from sequence 0 up to the live message, so it can catch up only
// while the live sequence number is at most CAPACITY - 1.

TEST(IngressRecoveryTest, JoinerCanStillCatchUpWhenTheWholeHistoryJustFitsInTheRing) {
    constexpr uint64_t CAPACITY = 1024;
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_ring_fits.dat", CAPACITY);
    for (uint64_t seq = 0; seq < CAPACITY - 1; ++seq) store.append(MakeMessage(seq));

    RetransmitServer<TestCommand> server(store, 45303);
    server.start();
    const NetworkConfig config = MakeConfig("239.255.1.3", 25203, 45303, false);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    const TestMessage live = MakeMessage(CAPACITY - 1); // fills the last free slot; sequence 0 is still there
    store.append(live);
    SendLive(config, live);

    ExpectInOrder(Drain(queue, CAPACITY), CAPACITY);
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    receiver.stop();
    server.stop();
}

TEST(IngressRecoveryTest, JoinerStopsWhenOneMoreMessageEvictsSequenceZero) {
    constexpr uint64_t CAPACITY = 1024;
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_ring_wrapped.dat", CAPACITY);
    for (uint64_t seq = 0; seq < CAPACITY; ++seq) store.append(MakeMessage(seq));

    RetransmitServer<TestCommand> server(store, 45304);
    server.start();
    const NetworkConfig config = MakeConfig("239.255.1.4", 25204, 45304, false);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    const TestMessage live = MakeMessage(CAPACITY); // overwrites the slot sequence 0 lived in
    store.append(live);
    SendLive(config, live);

    EXPECT_TRUE(WaitForUnrecoverableGap(receiver));

    TestCommand none{};
    EXPECT_FALSE(PopWithin(queue, none, NOTHING_ARRIVES_WAIT))
        << "a replica that cannot reach sequence 0 must deliver nothing, not a partial history";

    receiver.stop();
    server.stop();
}

// =====================================================================
// Retry after a partly lost retransmit reply (idle probe off)
// =====================================================================

TEST(IngressRecoveryTest, RetryAfterPartialBatchDoesNotDuplicateOrSkipRecords) {
    constexpr uint64_t HISTORY = 600;               // one full batch of 500, then 100
    constexpr uint32_t DELIVERED_BEFORE_LOSS = 100; // first reply stops after 100 of the 500
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_lossy.dat", 1024);
    for (uint64_t seq = 0; seq < HISTORY; ++seq) store.append(MakeMessage(seq));

    LossyRetransmitServer server(store, 45305, DELIVERED_BEFORE_LOSS);
    server.start();
    const NetworkConfig config = MakeConfig("239.255.1.5", 25205, 45305, false);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    const TestMessage live = MakeMessage(HISTORY);
    store.append(live);
    SendLive(config, live);

    ExpectInOrder(Drain(queue, HISTORY + 1), HISTORY + 1);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT)) << "a duplicate command followed the replay";
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    receiver.stop();
    server.stop();
}

// =====================================================================
// Duplicated, reordered and stale datagrams (idle probe off)
// =====================================================================

TEST(IngressRecoveryTest, DuplicateAndLateDatagramsAreNotAppliedTwice) {
    constexpr uint64_t TOTAL = 7;
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_duplicates.dat", 1024);
    for (uint64_t seq = 0; seq < TOTAL; ++seq) store.append(MakeMessage(seq));

    RetransmitServer<TestCommand> server(store, 45309);
    server.start();
    const NetworkConfig config = MakeConfig("239.255.1.10", 25210, 45309, false);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    // 1 arrives twice. 0 turns up again after 3 has been delivered. 5 arrives
    // before 4, so the receiver recovers 4 from the store, and the original 4
    // datagram then arrives late.
    for (uint64_t seq : { 0, 1, 1, 2, 0, 3, 5, 4, 6 }) SendLive(config, MakeMessage(seq));

    ExpectInOrder(Drain(queue, TOTAL), TOTAL);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT)) << "a datagram was applied a second time";
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    receiver.stop();
    server.stop();
}

// =====================================================================
// Idle probe: a replica that is behind finds out without a live datagram
// =====================================================================

TEST(IngressRecoveryTest, IdleProbeCatchesUpALateJoinerWithoutAnyLiveMessage) {
    constexpr uint64_t HISTORY = 2'000;
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_probe_late_join.dat", 4096);
    for (uint64_t seq = 0; seq < HISTORY; ++seq) store.append(MakeMessage(seq));

    RetransmitServer<TestCommand> server(store, 45306);
    server.start();
    const NetworkConfig config = MakeConfig("239.255.1.6", 25206, 45306, true);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();

    // No live datagram is ever sent for the history. The first idle timeout
    // asks the server and replays everything.
    ExpectInOrder(Drain(queue, HISTORY), HISTORY);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT)) << "a duplicate command followed the replay";

    // Caught up: the next live datagram is in order, with no gap and no duplicate.
    const TestMessage live = MakeMessage(HISTORY);
    store.append(live);
    SendLive(config, live);
    const auto next = Drain(queue, 1);
    ASSERT_EQ(size_t(1), next.size());
    EXPECT_EQ(PayloadFor(HISTORY), next[0].payload);
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT)) << "a duplicate command followed the live message";
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    receiver.stop();
    server.stop();
}

TEST(IngressRecoveryTest, IdleProbeRecoversTheLostTailOfABurst) {
    constexpr uint64_t DELIVERED_LIVE = 100;
    constexpr uint64_t LOST_TAIL = 50;
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_probe_tail.dat", 1024);
    RetransmitServer<TestCommand> server(store, 45307);
    server.start();
    const NetworkConfig config = MakeConfig("239.255.1.7", 25207, 45307, true);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    for (uint64_t seq = 0; seq < DELIVERED_LIVE; ++seq) {
        const TestMessage msg = MakeMessage(seq);
        store.append(msg);
        SendLive(config, msg);
    }
    ExpectInOrder(Drain(queue, DELIVERED_LIVE), DELIVERED_LIVE);

    // The last datagrams of the burst reach the store but never this replica,
    // and nothing is sent after them, so no gap is ever visible to it.
    for (uint64_t seq = DELIVERED_LIVE; seq < DELIVERED_LIVE + LOST_TAIL; ++seq) store.append(MakeMessage(seq));
    ExpectInOrder(Drain(queue, LOST_TAIL), LOST_TAIL, DELIVERED_LIVE);

    const uint64_t liveSeq = DELIVERED_LIVE + LOST_TAIL;
    const TestMessage live = MakeMessage(liveSeq);
    store.append(live);
    SendLive(config, live);
    const auto next = Drain(queue, 1);
    ASSERT_EQ(size_t(1), next.size());
    EXPECT_EQ(PayloadFor(liveSeq), next[0].payload);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT)) << "a duplicate command followed the live message";
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    receiver.stop();
    server.stop();
}

TEST(IngressRecoveryTest, IdleProbeStopsWhenTheHistoryWasEvicted) {
    constexpr uint64_t CAPACITY = 1024;
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_probe_wrapped.dat", CAPACITY);
    for (uint64_t seq = 0; seq <= CAPACITY; ++seq) store.append(MakeMessage(seq)); // the last one evicts sequence 0

    RetransmitServer<TestCommand> server(store, 45308);
    server.start();
    const NetworkConfig config = MakeConfig("239.255.1.8", 25208, 45308, true);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();

    EXPECT_TRUE(WaitForUnrecoverableGap(receiver));
    TestCommand none{};
    EXPECT_FALSE(PopWithin(queue, none, NOTHING_ARRIVES_WAIT))
        << "a replica that cannot reach sequence 0 must deliver nothing, not a partial history";

    receiver.stop();
    server.stop();
}

TEST(IngressRecoveryTest, UnreachableRetransmitServerDoesNotStopTheReplicaOrBlockLiveDelivery) {
    // Nothing is listening on the retransmit port.
    const NetworkConfig config = MakeConfig("239.255.1.9", 25209, 45310, true);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();
    std::this_thread::sleep_for(TWO_IDLE_TIMEOUTS); // at least two probes get no answer

    EXPECT_FALSE(receiver.hitUnrecoverableGap()) << "an unreachable server is not an unrecoverable gap";

    SendLive(config, MakeMessage(0));
    ExpectInOrder(Drain(queue, 1), 1);

    receiver.stop();
}
