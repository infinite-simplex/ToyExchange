// Ingress recovery: what happens when a replica joins late, restarts, loses
// the tail of a burst, loses part of a retransmit reply, or is sent the same
// datagram twice, and that it asks the kernel for the receive buffer it is
// configured to use.
//
// Core has no protocol or product command type of its own, so these tests use
// a minimal stand-in (TestCommand) with the real SequenceStore,
// RetransmitServer and MulticastIngressReceiver. The gateway is not needed
// here: it does two things per command, append to the store and then
// multicast, and these tests do those two steps by hand, always in that order.
// GatewayTesting.cpp tests the real gateway.
//
// A receiver finds out it is behind in two ways: a live datagram whose
// sequence number skips ahead (the "lazy" path), or a full receive timeout
// with nothing arriving (the idle probe, NetworkConfig::ingressIdleProbe).
// Tests that target the lazy path switch the probe off so they stay
// deterministic; the probe tests leave it on and send no live datagram.

#include "IngressTestSupport.hpp"
#include "RetransmitServer.hpp"
#include "SequenceStore.hpp"
#include "RetransmitMessage.hpp"
#include "ReceiveBuffer.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <set>
#include <thread>
#include <utility>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace ingress_test;

namespace {

std::set<uint64_t> SequenceRange(uint64_t first, uint64_t last) { // first .. last-1
    std::set<uint64_t> range;
    for (uint64_t seq = first; seq < last; ++seq) range.insert(seq);
    return range;
}

// A retransmit server that damages its replies the way a network can. It
// answers like the real server, from whatever start sequence and count a
// request asks for, in datagrams of several records, with three faults on top:
//   lostOnce          the network drops a reply datagram the first time the
//                     server sends it, if the datagram holds any of these
//                     sequence numbers. A retry gets them.
//   duplicateReplies  every reply datagram arrives twice.
//   staleOverlap      before the real reply, a stale datagram arrives that
//                     starts 10 records before the requested start and covers
//                     20 records, as a late copy of an earlier attempt would.
class UnreliableRetransmitServer {
public:
    UnreliableRetransmitServer(SequenceStore<TestCommand>& store, uint16_t port,
        std::set<uint64_t> lostOnce, bool duplicateReplies = false, bool staleOverlap = false)
        : m_store(store), m_port(port), m_lostOnce(std::move(lostOnce)),
          m_duplicateReplies(duplicateReplies), m_staleOverlap(staleOverlap) {
    }
    ~UnreliableRetransmitServer() { stop(); }

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
        m_thread = std::thread(&UnreliableRetransmitServer::serve, this);
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

            if (m_staleOverlap && req.startSeq >= 10) sendRecords(req.startSeq - 10, 20, from, fromLen, false);

            const uint32_t reply = std::min<uint32_t>(req.count, RETRANSMIT_BATCH);
            uint32_t sent = 0;
            while (sent < reply) {
                const uint32_t inPacket = std::min<uint32_t>(reply - sent, RECORDS_PER_DATAGRAM);
                const uint32_t available = sendRecords(req.startSeq + sent, inPacket, from, fromLen, true);
                sent += available;
                if (available < inPacket) { // the store has nothing further
                    RetransmitReplyHeader end{ RetransmitStatus::NOT_YET_ARRIVED, {}, 0, req.startSeq + sent };
                    sendto(m_fd, &end, sizeof(end), 0, (sockaddr*)&from, fromLen);
                    break;
                }
            }
        }
    }

    // Sends up to `count` stored records from `first` as one datagram and
    // returns how many it found. `faulty` applies the lostOnce and
    // duplicateReplies faults; a stale datagram is never faulty.
    uint32_t sendRecords(uint64_t first, uint32_t count, const sockaddr_in& to, socklen_t toLen, bool faulty) {
        std::vector<char> packet(sizeof(RetransmitReplyHeader) + count * sizeof(TestMessage));
        uint32_t found = 0;
        bool lost = false;
        while (found < count) {
            TestMessage record{};
            if (m_store.read(first + found, record) != SequenceStore<TestCommand>::ReadResult::OK) break;
            std::memcpy(packet.data() + sizeof(RetransmitReplyHeader) + found * sizeof(TestMessage),
                &record, sizeof(record));
            if (faulty && m_lostOnce.erase(first + found) != 0) lost = true;
            ++found;
        }
        if (found == 0) return 0;

        const RetransmitReplyHeader hdr{ RetransmitStatus::OK, {}, found, first };
        std::memcpy(packet.data(), &hdr, sizeof(hdr));
        const size_t bytes = sizeof(hdr) + found * sizeof(TestMessage);
        if (!lost) { // otherwise dropped on the wire
            sendto(m_fd, packet.data(), bytes, 0, (const sockaddr*)&to, toLen);
            if (faulty && m_duplicateReplies) sendto(m_fd, packet.data(), bytes, 0, (const sockaddr*)&to, toLen);
        }
        return found;
    }

    SequenceStore<TestCommand>& m_store;
    uint16_t m_port;
    std::set<uint64_t> m_lostOnce; // touched only by the server thread
    bool m_duplicateReplies;
    bool m_staleOverlap;
    static constexpr uint32_t RECORDS_PER_DATAGRAM =
        static_cast<uint32_t>(RecordsPerDatagram<TestCommand>(DEFAULT_DATAGRAM_MAX_BYTES, sizeof(RetransmitReplyHeader)));
    int m_fd{ -1 };
    std::atomic<bool> m_running{ false };
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
// Damaged retransmit replies (idle probe off, except where stated)
// =====================================================================

// A late joiner finds sequence 0 .. HISTORY-1 in the store, learns it is behind
// from the live datagram HISTORY, and replays the history through `server`.
// The history is one full batch of 500, then 100.
static void ExpectReplayThrough(UnreliableRetransmitServer& server, SequenceStore<TestCommand>& store,
    const char* group, uint16_t multicastPort, uint16_t retransmitPort) {
    constexpr uint64_t HISTORY = 600;
    for (uint64_t seq = 0; seq < HISTORY; ++seq) store.append(MakeMessage(seq));

    server.start();
    const NetworkConfig config = MakeConfig(group, multicastPort, retransmitPort, false);
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

TEST(IngressRecoveryTest, RetryAfterPartialBatchDoesNotDuplicateOrSkipRecords) {
    // The network drops the end of the first reply: records 100 .. 499.
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_lossy.dat", 1024);
    UnreliableRetransmitServer server(store, 45305, SequenceRange(100, 500));
    ExpectReplayThrough(server, store, "239.255.1.5", 25205, 45305);
}

TEST(IngressRecoveryTest, RecordLostInTheMiddleOfAReplyIsNotReplacedByALaterOne) {
    // The network drops records 100 .. 123 but delivers 124 .. 499 after them.
    // The receiver must not take record 124 as the one it still waits for.
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_lossy_middle.dat", 1024);
    UnreliableRetransmitServer server(store, 45311, SequenceRange(100, 124));
    ExpectReplayThrough(server, store, "239.255.1.11", 25211, 45311);
}

TEST(IngressRecoveryTest, DuplicatedRetransmitRepliesAreNotAppliedTwice) {
    // Every record datagram arrives twice. The copy of record 499 is still in
    // the socket when the second batch starts, so it is a stale reply there.
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_lossy_dup.dat", 1024);
    UnreliableRetransmitServer server(store, 45312, {}, true);
    ExpectReplayThrough(server, store, "239.255.1.12", 25212, 45312);
}

TEST(IngressRecoveryTest, IdleProbeResumesAtARecordLostInTheMiddleOfAReply) {
    // No live datagram: the idle probe finds the ten stored records. The network
    // drops record 3. The first probe delivers 0 .. 2 and ignores 4 .. 9, and
    // the next probe, one idle timeout later, delivers 3 .. 9.
    constexpr uint64_t STORED = 10;
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_probe_middle.dat", 1024);
    for (uint64_t seq = 0; seq < STORED; ++seq) store.append(MakeMessage(seq));

    UnreliableRetransmitServer server(store, 45313, { 3 });
    server.start();
    const NetworkConfig config = MakeConfig("239.255.1.13", 25213, 45313, true);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();

    ExpectInOrder(Drain(queue, STORED), STORED);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, TWO_IDLE_TIMEOUTS)) << "a duplicate command followed the catch-up";
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

// =====================================================================
// Receive buffer
// =====================================================================

TEST(IngressRecoveryTest, ReceiveBufferDefaultsToFourMebibytes) {
    EXPECT_EQ(4 * 1024 * 1024, NetworkConfig{}.ingressReceiveBufferBytes);
}

TEST(IngressRecoveryTest, ReceiverAppliesTheConfiguredReceiveBuffer) {
    constexpr int REQUESTED = 64 * 1024; // far below any net.core.rmem_max, so the kernel grants it in full
    NetworkConfig config = MakeConfig("239.255.1.30", 25230, 45330, false);
    config.ingressReceiveBufferBytes = REQUESTED;
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();

    EXPECT_EQ(REQUESTED, receiver.receiveBufferBytes());

    receiver.stop();
}

TEST(IngressRecoveryTest, ReceiveBufferHelperReportsWhenTheKernelCapClipsTheRequest) {
    constexpr int ABSURD = 1 << 30; // 1 GiB, above any real net.core.rmem_max
    const int fd = socket(AF_INET, SOCK_DGRAM, 0);
    ASSERT_GE(fd, 0);

    const int granted = SetReceiveBuffer(fd, ABSURD);
    EXPECT_GT(granted, 0);
    EXPECT_LT(granted, ABSURD) << "the helper must report the clipped size, not the requested one";

    close(fd);
}

// =====================================================================
// Datagrams that carry several records (idle probe off)
// =====================================================================

TEST(IngressRecoveryTest, OneDatagramCanCarryManyRecords) {
    const NetworkConfig config = MakeConfig("239.255.1.50", 25250, 45350, false);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    SendDatagram(config, Messages(0, 30));
    SendDatagram(config, Messages(30, 31)); // a batch of one is still a batch
    SendDatagram(config, Messages(31, 40));

    ExpectInOrder(Drain(queue, 40), 40);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT));
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    receiver.stop();
}

TEST(IngressRecoveryTest, RecordsInsideADatagramThatWereAlreadyDeliveredAreSkipped) {
    const NetworkConfig config = MakeConfig("239.255.1.51", 25251, 45351, false);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    SendDatagram(config, Messages(0, 5));
    SendDatagram(config, Messages(3, 9));  // 3 and 4 were delivered, 5 .. 8 are new
    SendDatagram(config, Messages(9, 12));
    SendDatagram(config, Messages(0, 12)); // a whole stale copy

    ExpectInOrder(Drain(queue, 12), 12);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT)) << "a record was applied twice";
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    receiver.stop();
}

TEST(IngressRecoveryTest, DatagramThatStartsPastTheNextExpectedRecordRecoversTheGapFirst) {
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_batch_gap.dat", 1024);
    for (uint64_t seq = 0; seq < 10; ++seq) store.append(MakeMessage(seq));
    RetransmitServer<TestCommand> server(store, 45352);
    server.start();
    const NetworkConfig config = MakeConfig("239.255.1.52", 25252, 45352, false);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    SendDatagram(config, Messages(6, 10)); // records 0 .. 5 never arrived

    ExpectInOrder(Drain(queue, 10), 10);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT));
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    receiver.stop();
    server.stop();
}

TEST(IngressRecoveryTest, DatagramThatIsNotAWholeNumberOfRecordsIsDropped) {
    const NetworkConfig config = MakeConfig("239.255.1.53", 25253, 45353, false);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    SendDatagram(config, Messages(0, 3));
    std::vector<char> torn(3 * sizeof(TestMessage) + 5); // three records and 5 stray bytes
    const std::vector<TestMessage> records = Messages(3, 6);
    std::memcpy(torn.data(), records.data(), 3 * sizeof(TestMessage));
    SendRaw(config, torn.data(), torn.size());
    SendDatagram(config, Messages(3, 5));

    ExpectInOrder(Drain(queue, 5), 5);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT)) << "a torn datagram must not deliver anything";
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    receiver.stop();
}

// =====================================================================
// Batched retransmit replies
// =====================================================================

namespace {

struct ReplyDatagram {
    RetransmitReplyHeader header;
    size_t bytes;
    std::vector<TestMessage> records;
};

// Sends one request to a retransmit server and collects every reply datagram
// until the server has been quiet for 200 ms.
std::vector<ReplyDatagram> RequestAndCollect(uint16_t port, uint64_t startSeq, uint32_t count) {
    std::vector<ReplyDatagram> replies;
    const int fd = socket(AF_INET, SOCK_DGRAM, 0);
    timeval timeout{};
    timeout.tv_usec = 200'000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in server{};
    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &server.sin_addr);

    const RetransmitRequest req{ startSeq, count };
    sendto(fd, &req, sizeof(req), 0, (sockaddr*)&server, sizeof(server));

    std::vector<char> buffer(65535);
    while (true) {
        const ssize_t n = recv(fd, buffer.data(), buffer.size(), 0);
        if (n < static_cast<ssize_t>(sizeof(RetransmitReplyHeader))) break;
        ReplyDatagram reply{};
        reply.bytes = static_cast<size_t>(n);
        std::memcpy(&reply.header, buffer.data(), sizeof(reply.header));
        for (uint32_t i = 0; i < reply.header.count; ++i) {
            TestMessage record{};
            std::memcpy(&record, buffer.data() + sizeof(RetransmitReplyHeader) + i * sizeof(TestMessage), sizeof(record));
            reply.records.push_back(record);
        }
        replies.push_back(std::move(reply));
    }
    close(fd);
    return replies;
}

} // namespace

TEST(IngressRecoveryTest, RetransmitServerAnswersARangeInFewDatagrams) {
    constexpr uint32_t RECORDS = 100;
    const size_t perDatagram = RecordsPerDatagram<TestCommand>(DEFAULT_DATAGRAM_MAX_BYTES, sizeof(RetransmitReplyHeader));
    ASSERT_GT(perDatagram, 1u) << "the test record must fit several times in one datagram";

    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_reply_batch.dat", 1024);
    for (uint64_t seq = 0; seq < RECORDS; ++seq) store.append(MakeMessage(seq));
    RetransmitServer<TestCommand> server(store, 45360);
    server.start();

    const std::vector<ReplyDatagram> replies = RequestAndCollect(45360, 0, RECORDS);

    EXPECT_EQ((RECORDS + perDatagram - 1) / perDatagram, replies.size());
    uint64_t next = 0;
    for (const ReplyDatagram& reply : replies) {
        EXPECT_EQ(RetransmitStatus::OK, reply.header.status);
        EXPECT_LE(reply.bytes, DEFAULT_DATAGRAM_MAX_BYTES);
        EXPECT_EQ(sizeof(RetransmitReplyHeader) + reply.header.count * sizeof(TestMessage), reply.bytes);
        EXPECT_EQ(next, reply.header.firstSequence) << "records must continue where the previous datagram ended";
        for (const TestMessage& record : reply.records) {
            EXPECT_EQ(next, record.sequenceNumber);
            EXPECT_EQ(PayloadFor(next), record.command.payload);
            ++next;
        }
    }
    EXPECT_EQ(RECORDS, next);

    server.stop();
}

TEST(IngressRecoveryTest, RetransmitServerEndsAShortRangeWithOneStatusDatagram) {
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_reply_status.dat", 64);
    for (uint64_t seq = 0; seq < 100; ++seq) store.append(MakeMessage(seq)); // ring of 64: 36 .. 99 remain
    RetransmitServer<TestCommand> server(store, 45361);
    server.start();

    // 95 .. 99 exist, 100 has not been sequenced yet.
    const std::vector<ReplyDatagram> tail = RequestAndCollect(45361, 95, 20);
    ASSERT_EQ(2u, tail.size());
    EXPECT_EQ(RetransmitStatus::OK, tail[0].header.status);
    EXPECT_EQ(95u, tail[0].header.firstSequence);
    EXPECT_EQ(5u, tail[0].header.count);
    EXPECT_EQ(RetransmitStatus::NOT_YET_ARRIVED, tail[1].header.status);
    EXPECT_EQ(0u, tail[1].header.count);
    EXPECT_EQ(100u, tail[1].header.firstSequence);

    // 10 was evicted: the reply is the status alone.
    const std::vector<ReplyDatagram> evicted = RequestAndCollect(45361, 10, 5);
    ASSERT_EQ(1u, evicted.size());
    EXPECT_EQ(RetransmitStatus::TOO_OLD, evicted[0].header.status);
    EXPECT_EQ(0u, evicted[0].header.count);
    EXPECT_EQ(10u, evicted[0].header.firstSequence);

    server.stop();
}

TEST(IngressRecoveryTest, StaleReplyThatOverlapsTheNextNeededRecordIsUsedFromThatRecordOnward) {
    // Before the real reply to each request that starts at 10 or later, the fake
    // server sends a late copy of 20 older records, 10 of them before the
    // start. The receiver must take only the ones it still needs.
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_lossy_overlap.dat", 1024);
    UnreliableRetransmitServer server(store, 45314, {}, false, true);
    ExpectReplayThrough(server, store, "239.255.1.14", 25214, 45314);
}

// =====================================================================
// Receiver spin window (NetworkConfig::ingressSpinMicros)
// =====================================================================

TEST(IngressRecoveryTest, ReceiverSpinningIsOffByDefault) {
    EXPECT_EQ(0u, NetworkConfig{}.ingressSpinMicros);
}

TEST(IngressRecoveryTest, SpinningReceiverDeliversInOrderAndStillWakesAfterAQuietSpell) {
    NetworkConfig config = MakeConfig("239.255.1.60", 25260, 45370, false);
    config.ingressSpinMicros = 500;
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    for (uint64_t seq = 0; seq < 200; ++seq) SendLive(config, MakeMessage(seq)); // back to back: the receiver spins between them
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // 200 times the window: it blocks again
    for (uint64_t seq = 200; seq < 400; seq += 5) SendDatagram(config, Messages(seq, seq + 5));

    ExpectInOrder(Drain(queue, 400), 400);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT));
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    const auto stopStarted = std::chrono::steady_clock::now();
    receiver.stop();
    EXPECT_LT(std::chrono::steady_clock::now() - stopStarted, std::chrono::seconds(1))
        << "a spinning receiver must still stop promptly";
}

TEST(IngressRecoveryTest, SpinningReceiverStillProbesWhenIdle) {
    SequenceStore<TestCommand> store("/tmp/toyexchange_ingress_spin_probe.dat", 1024);
    for (uint64_t seq = 0; seq < 10; ++seq) store.append(MakeMessage(seq));
    RetransmitServer<TestCommand> server(store, 45371);
    server.start();

    NetworkConfig config = MakeConfig("239.255.1.61", 25261, 45371, true);
    config.ingressSpinMicros = 500;
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    receiver.start(); // no live datagram: nothing to spin after, so the idle probe must still fire

    ExpectInOrder(Drain(queue, 10), 10);
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    receiver.stop();
    server.stop();
}
