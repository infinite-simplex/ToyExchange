// NetworkGateway end to end, with a stand-in protocol: TCP clients send
// fixed-size frames, the real gateway sequences them, appends them to its
// store and multicasts them, and the real MulticastIngressReceiver hands them
// to a queue.
//
// Core has no wire protocol of its own, so FixedFrameHandler is the smallest
// handler that satisfies the NetworkGateway contract: a frame is 8 bytes, the
// value UINT64_MAX is a structurally broken frame, anything else is a command
// payload. What these tests pin down is what the gateway does, whatever the
// protocol: sequence numbers, the store-before-multicast rule, how many
// datagrams it needs, and how it treats half frames and broken frames.

#include "IngressTestSupport.hpp"
#include "NetworkGateway.hpp"
#include "SequenceStore.hpp"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace ingress_test;

namespace {

constexpr uint64_t BAD_FRAME = UINT64_MAX;
constexpr size_t FRAME_BYTES = sizeof(uint64_t);

struct FixedFrameHandler {
    static constexpr size_t PARSE_FATAL_ERROR = SIZE_MAX;

    size_t validateAndParse(const char* buf, size_t availableLen, TestCommand& out) {
        if (availableLen < FRAME_BYTES) return 0; // half a frame: wait for the rest
        uint64_t payload = 0;
        std::memcpy(&payload, buf, FRAME_BYTES);
        if (payload == BAD_FRAME) return PARSE_FATAL_ERROR;
        out.payload = payload;
        return FRAME_BYTES;
    }
};

using TestGateway = NetworkGateway<FixedFrameHandler, TestCommand>;

NetworkConfig GatewayConfig(const char* group, uint16_t multicastPort, uint16_t tcpPort) {
    NetworkConfig config = MakeConfig(group, multicastPort, /*retransmitPort*/ 0, /*idleProbe*/ false);
    config.ingressListenPort = tcpPort;
    return config;
}

std::string FreshStorePath(const char* name) {
    const std::string path = std::string("/tmp/toyexchange_gateway_") + name + ".dat";
    std::remove(path.c_str());
    return path;
}

int Connect(uint16_t tcpPort) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(tcpPort);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// One write() of `count` frames whose payloads are PayloadFor(first) ... so a
// client that is first to send gets sequence numbers that match the payloads.
void WriteFrames(int fd, uint64_t first, uint64_t count) {
    std::vector<uint64_t> frames;
    for (uint64_t i = 0; i < count; ++i) frames.push_back(PayloadFor(first + i));
    ASSERT_EQ(static_cast<ssize_t>(frames.size() * FRAME_BYTES),
        send(fd, frames.data(), frames.size() * FRAME_BYTES, 0));
}

// Joins the gateway's multicast group like a replica does, but only watches:
// it records how many records each datagram carried and, at the moment a
// datagram arrives, checks that every record in it is already readable from the
// gateway's store. A replica that asks for a retransmit right after seeing a
// datagram depends on exactly that.
class DatagramTap {
public:
    DatagramTap(const NetworkConfig& config, const SequenceStore<TestCommand>& store)
        : m_store(store) {
        m_fd = socket(AF_INET, SOCK_DGRAM, 0);
        int opt = 1;
        setsockopt(m_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        timeval timeout{};
        timeout.tv_usec = 50'000;
        setsockopt(m_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        sockaddr_in bindAddr{};
        bindAddr.sin_family = AF_INET;
        bindAddr.sin_port = htons(config.multicastPort);
        bindAddr.sin_addr.s_addr = INADDR_ANY;
        bind(m_fd, (sockaddr*)&bindAddr, sizeof(bindAddr));
        ip_mreq mreq{};
        inet_pton(AF_INET, config.multicastIp.c_str(), &mreq.imr_multiaddr);
        mreq.imr_interface.s_addr = INADDR_ANY;
        setsockopt(m_fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
        m_running.store(true);
        m_thread = std::thread(&DatagramTap::run, this);
    }
    ~DatagramTap() {
        m_running.store(false);
        if (m_thread.joinable()) m_thread.join();
        close(m_fd);
    }

    std::vector<size_t> recordsPerDatagram() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_recordsPerDatagram;
    }
    uint64_t recordsMissingFromStore() const { return m_missingFromStore.load(); }
    uint64_t malformedDatagrams() const { return m_malformed.load(); }

private:
    void run() {
        std::vector<char> buffer(65535);
        while (m_running.load()) {
            const ssize_t len = recv(m_fd, buffer.data(), buffer.size(), 0);
            if (len <= 0) continue;
            if (len % sizeof(TestMessage) != 0) {
                ++m_malformed;
                continue;
            }
            const size_t count = static_cast<size_t>(len) / sizeof(TestMessage);
            for (size_t i = 0; i < count; ++i) {
                TestMessage msg{};
                std::memcpy(&msg, buffer.data() + i * sizeof(TestMessage), sizeof(msg));
                TestMessage stored{};
                if (m_store.read(msg.sequenceNumber, stored) != SequenceStore<TestCommand>::ReadResult::OK) {
                    ++m_missingFromStore;
                }
            }
            std::lock_guard<std::mutex> lock(m_mutex);
            m_recordsPerDatagram.push_back(count);
        }
    }

    const SequenceStore<TestCommand>& m_store;
    int m_fd{ -1 };
    std::atomic<bool> m_running{ false };
    std::thread m_thread;
    mutable std::mutex m_mutex;
    std::vector<size_t> m_recordsPerDatagram;
    std::atomic<uint64_t> m_missingFromStore{ 0 };
    std::atomic<uint64_t> m_malformed{ 0 };
};

} // namespace

TEST(GatewayTest, CommandsFromOneClientReachAReplicaInOrderAndAreStored) {
    constexpr uint64_t COMMANDS = 200;
    const NetworkConfig config = GatewayConfig("239.255.1.40", 25240, 47010);
    FixedFrameHandler handler;
    TestGateway gateway(handler, config, FreshStorePath("in_order"), 1024);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    gateway.start();
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    const int client = Connect(47010);
    ASSERT_GE(client, 0);
    for (uint64_t sent = 0; sent < COMMANDS; sent += 40) WriteFrames(client, sent, 40);

    const std::vector<TestCommand> got = Drain(queue, COMMANDS);
    ExpectInOrder(got, COMMANDS);
    ASSERT_EQ(COMMANDS, got.size());
    for (uint64_t seq = 0; seq < got.size(); ++seq) {
        EXPECT_EQ(static_cast<TraceId>(seq), got[seq].trace_id) << "the sequence number is the trace id";
        EXPECT_EQ(got.front().sessionId, got[seq].sessionId) << "one connection is one session";
        TestMessage stored{};
        EXPECT_EQ(SequenceStore<TestCommand>::ReadResult::OK, gateway.sequenceStore().read(seq, stored));
        EXPECT_EQ(PayloadFor(seq), stored.command.payload);
    }
    EXPECT_FALSE(receiver.hitUnrecoverableGap());

    close(client);
    receiver.stop();
    gateway.stop();
}

TEST(GatewayTest, FrameSplitAcrossReadsIsParsedOnce) {
    const NetworkConfig config = GatewayConfig("239.255.1.41", 25241, 47011);
    FixedFrameHandler handler;
    TestGateway gateway(handler, config, FreshStorePath("split_frame"), 1024);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    gateway.start();
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    const int client = Connect(47011);
    ASSERT_GE(client, 0);
    const uint64_t frames[2] = { PayloadFor(0), PayloadFor(1) };
    const char* bytes = reinterpret_cast<const char*>(frames);
    ASSERT_EQ(3, send(client, bytes, 3, 0)); // 3 bytes of frame 0
    std::this_thread::sleep_for(std::chrono::milliseconds(50)); // the gateway reads them and must wait
    ASSERT_EQ(static_cast<ssize_t>(sizeof(frames) - 3), send(client, bytes + 3, sizeof(frames) - 3, 0));

    ExpectInOrder(Drain(queue, 2), 2);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT));

    close(client);
    receiver.stop();
    gateway.stop();
}

TEST(GatewayTest, EachConnectionGetsItsOwnSessionId) {
    const NetworkConfig config = GatewayConfig("239.255.1.42", 25242, 47012);
    FixedFrameHandler handler;
    TestGateway gateway(handler, config, FreshStorePath("sessions"), 1024);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    gateway.start();
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    const int a = Connect(47012);
    const int b = Connect(47012);
    ASSERT_GE(a, 0);
    ASSERT_GE(b, 0);
    WriteFrames(a, 0, 3);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    WriteFrames(b, 3, 3);

    const std::vector<TestCommand> got = Drain(queue, 6);
    ExpectInOrder(got, 6);
    EXPECT_EQ(got[0].sessionId, got[1].sessionId);
    EXPECT_EQ(got[0].sessionId, got[2].sessionId);
    EXPECT_EQ(got[3].sessionId, got[4].sessionId);
    EXPECT_EQ(got[3].sessionId, got[5].sessionId);
    EXPECT_NE(got[0].sessionId, got[3].sessionId);

    close(a);
    close(b);
    receiver.stop();
    gateway.stop();
}

TEST(GatewayTest, BrokenFrameClosesTheConnectionButEarlierFramesAreStillSequenced) {
    const NetworkConfig config = GatewayConfig("239.255.1.43", 25243, 47013);
    FixedFrameHandler handler;
    TestGateway gateway(handler, config, FreshStorePath("broken_frame"), 1024);
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    gateway.start();
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    const int client = Connect(47013);
    ASSERT_GE(client, 0);
    const uint64_t frames[4] = { PayloadFor(0), PayloadFor(1), BAD_FRAME, PayloadFor(3) };
    ASSERT_EQ(static_cast<ssize_t>(sizeof(frames)), send(client, frames, sizeof(frames), 0));

    ExpectInOrder(Drain(queue, 2), 2);
    TestCommand extra{};
    EXPECT_FALSE(PopWithin(queue, extra, NOTHING_ARRIVES_WAIT)) << "nothing after the broken frame may be sequenced";

    timeval timeout{};
    timeout.tv_sec = 1;
    setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    char byte = 0;
    EXPECT_EQ(0, recv(client, &byte, 1, 0)) << "the gateway must close a connection that sent a broken frame";

    close(client);
    receiver.stop();
    gateway.stop();
}

TEST(GatewayTest, EveryRecordIsInTheStoreBeforeItsDatagramArrives) {
    constexpr uint64_t COMMANDS = 500;
    const NetworkConfig config = GatewayConfig("239.255.1.44", 25244, 47014);
    FixedFrameHandler handler;
    TestGateway gateway(handler, config, FreshStorePath("store_first"), 1024);
    DatagramTap tap(config, gateway.sequenceStore());
    gateway.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    const int client = Connect(47014);
    ASSERT_GE(client, 0);
    for (uint64_t sent = 0; sent < COMMANDS; sent += 50) WriteFrames(client, sent, 50);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    auto total = [&] { size_t n = 0; for (size_t c : tap.recordsPerDatagram()) n += c; return n; };
    while (total() < COMMANDS && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_EQ(COMMANDS, total());
    EXPECT_EQ(0u, tap.recordsMissingFromStore()) << "a datagram reached the network before its record was stored";
    EXPECT_EQ(0u, tap.malformedDatagrams());

    close(client);
    gateway.stop();
}

TEST(GatewayTest, CommandsFromOneReadShareFewDatagrams) {
    constexpr uint64_t COMMANDS = 70; // one write of 560 bytes: the gateway reads it in one go
    const size_t recordsPerDatagram = NetworkConfig{}.ingressDatagramMaxBytes / sizeof(TestMessage);
    ASSERT_GT(recordsPerDatagram, 1u) << "the test record must fit several times in one datagram";

    const NetworkConfig config = GatewayConfig("239.255.1.45", 25245, 47015);
    FixedFrameHandler handler;
    TestGateway gateway(handler, config, FreshStorePath("batching"), 1024);
    DatagramTap tap(config, gateway.sequenceStore());
    SPSCQueue<TestCommand> queue;
    MulticastIngressReceiver<TestCommand> receiver(queue, config);
    gateway.start();
    receiver.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    const int client = Connect(47015);
    ASSERT_GE(client, 0);
    WriteFrames(client, 0, COMMANDS);

    ExpectInOrder(Drain(queue, COMMANDS), COMMANDS); // the receiver unpacks every datagram
    const std::vector<size_t> perDatagram = tap.recordsPerDatagram();
    size_t records = 0;
    for (size_t count : perDatagram) {
        EXPECT_LE(count, recordsPerDatagram) << "a datagram is larger than ingressDatagramMaxBytes";
        records += count;
    }
    EXPECT_EQ(COMMANDS, records);
    EXPECT_LE(perDatagram.size(), 5u) << COMMANDS << " commands from one read took " << perDatagram.size()
        << " datagrams; one datagram per command is the old behaviour";

    close(client);
    receiver.stop();
    gateway.stop();
}

TEST(GatewayTest, ACommandThatArrivesAloneIsSentAtOnceAndNeverWaitsForOthers) {
    const NetworkConfig config = GatewayConfig("239.255.1.46", 25246, 47016);
    FixedFrameHandler handler;
    TestGateway gateway(handler, config, FreshStorePath("lone_command"), 1024);
    DatagramTap tap(config, gateway.sequenceStore());
    gateway.start();
    std::this_thread::sleep_for(GROUP_JOIN_WAIT);

    const int client = Connect(47016);
    ASSERT_GE(client, 0);

    auto waitForDatagrams = [&](size_t expected) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
        while (tap.recordsPerDatagram().size() < expected && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return tap.recordsPerDatagram();
    };

    WriteFrames(client, 0, 1);
    std::vector<size_t> seen = waitForDatagrams(1);
    ASSERT_EQ(1u, seen.size()) << "a lone command must not wait for others to fill a datagram";
    EXPECT_EQ(1u, seen[0]);

    std::this_thread::sleep_for(std::chrono::milliseconds(50)); // a quiet spell, then another lone command
    WriteFrames(client, 1, 1);
    seen = waitForDatagrams(2);
    ASSERT_EQ(2u, seen.size());
    EXPECT_EQ(1u, seen[1]);

    close(client);
    gateway.stop();
}

// =====================================================================
// Private output to a client that cannot keep up
// =====================================================================

namespace {

int ConnectWithReceiveBuffer(uint16_t tcpPort, int receiveBufferBytes) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &receiveBufferBytes, sizeof(receiveBufferBytes)); // before connect, so it sticks
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(tcpPort);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

constexpr size_t OUTPUT_FRAME_BYTES = 1024;

// A frame that can be checked: a counter, then bytes derived from it.
std::string MakeOutputFrame(uint64_t counter) {
    std::string frame(OUTPUT_FRAME_BYTES, '\0');
    std::memcpy(frame.data(), &counter, sizeof(counter));
    for (size_t i = sizeof(counter); i < frame.size(); ++i) frame[i] = static_cast<char>((counter + i) & 0xFF);
    return frame;
}

bool FrameIsIntact(const char* bytes, uint64_t& counter) {
    std::memcpy(&counter, bytes, sizeof(counter));
    for (size_t i = sizeof(counter); i < OUTPUT_FRAME_BYTES; ++i) {
        if (bytes[i] != static_cast<char>((counter + i) & 0xFF)) return false;
    }
    return true;
}

// Reads from `client` until the gateway has nothing left to flush and the socket has gone quiet,
// asking the gateway to flush as it goes, as EgressGateway does.
std::vector<char> ReadEverything(int client, TestGateway& gateway) {
    std::vector<char> got;
    std::vector<char> buffer(64 * 1024);
    auto quietSince = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - quietSince < std::chrono::milliseconds(500)) {
        gateway.flushPendingSessions();
        const ssize_t n = recv(client, buffer.data(), buffer.size(), MSG_DONTWAIT);
        if (n > 0) {
            got.insert(got.end(), buffer.begin(), buffer.begin() + n);
            quietSince = std::chrono::steady_clock::now();
        }
        else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    return got;
}

} // namespace

TEST(GatewayTest, OutputForAClientThatIsNotReadingIsKeptWholeAndInOrder) {
    constexpr uint64_t FRAMES = 12'000; // 12 MB: more than a loopback socket buffer holds
    NetworkConfig config = GatewayConfig("239.255.1.47", 25247, 47017);
    config.sessionOutboundBufferBytes = 64 * 1024 * 1024;
    FixedFrameHandler handler;
    TestGateway gateway(handler, config, FreshStorePath("slow_client_kept"), 1024);
    gateway.start();

    const int client = ConnectWithReceiveBuffer(47017, 4096);
    ASSERT_GE(client, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(150)); // the first session id is 1

    for (uint64_t n = 0; n < FRAMES; ++n) {
        const std::string frame = MakeOutputFrame(n);
        ASSERT_TRUE(gateway.sendToSession(1, frame.data(), frame.size())) << "frame " << n << " was refused";
    }
    EXPECT_GT(gateway.pendingOutputBytes(), 0u) << "the socket must have been full, or this test proves nothing";

    const std::vector<char> got = ReadEverything(client, gateway);
    ASSERT_EQ(FRAMES * OUTPUT_FRAME_BYTES, got.size());
    for (uint64_t n = 0; n < FRAMES; ++n) {
        uint64_t counter = 0;
        if (!FrameIsIntact(got.data() + n * OUTPUT_FRAME_BYTES, counter) || counter != n) {
            ADD_FAILURE() << "frame " << n << " is cut, mixed with another, or out of order (found " << counter << ")";
            break;
        }
    }
    EXPECT_EQ(0u, gateway.pendingOutputBytes());
    EXPECT_EQ(0u, gateway.droppedOutputBytes());

    close(client);
    gateway.stop();
}

TEST(GatewayTest, OutputBeyondTheLimitIsDroppedWholeAndCounted) {
    constexpr uint64_t FRAMES = 12'000;
    NetworkConfig config = GatewayConfig("239.255.1.48", 25248, 47018);
    config.sessionOutboundBufferBytes = 64 * 1024;
    FixedFrameHandler handler;
    TestGateway gateway(handler, config, FreshStorePath("slow_client_dropped"), 1024);
    gateway.start();

    const int client = ConnectWithReceiveBuffer(47018, 4096);
    ASSERT_GE(client, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    uint64_t refused = 0;
    for (uint64_t n = 0; n < FRAMES; ++n) {
        const std::string frame = MakeOutputFrame(n);
        if (!gateway.sendToSession(1, frame.data(), frame.size())) ++refused;
    }
    ASSERT_GT(refused, 0u) << "the limit must have been reached, or this test proves nothing";
    EXPECT_EQ(refused * OUTPUT_FRAME_BYTES, gateway.droppedOutputBytes()) << "a call is dropped whole";

    const std::vector<char> got = ReadEverything(client, gateway);
    ASSERT_EQ(0u, got.size() % OUTPUT_FRAME_BYTES) << "no frame may be cut short";
    const uint64_t received = got.size() / OUTPUT_FRAME_BYTES;
    EXPECT_EQ(FRAMES - refused, received) << "every call that was accepted must arrive";
    uint64_t previous = 0;
    for (uint64_t n = 0; n < received; ++n) {
        uint64_t counter = 0;
        if (!FrameIsIntact(got.data() + n * OUTPUT_FRAME_BYTES, counter) || (n > 0 && counter <= previous)) {
            ADD_FAILURE() << "frame at position " << n << " is damaged or out of order (counter " << counter << ")";
            break;
        }
        previous = counter;
    }

    close(client);
    gateway.stop();
}

TEST(GatewayTest, OutputForASessionThatDoesNotExistIsRefusedQuietly) {
    const NetworkConfig config = GatewayConfig("239.255.1.49", 25249, 47019);
    FixedFrameHandler handler;
    TestGateway gateway(handler, config, FreshStorePath("no_such_session"), 1024);
    gateway.start();
    const std::string frame = MakeOutputFrame(0);
    EXPECT_FALSE(gateway.sendToSession(999, frame.data(), frame.size()));
    EXPECT_EQ(0u, gateway.droppedOutputBytes()) << "an unknown session is not a slow one";
    gateway.stop();
}
