#pragma once
// Shared by the Core ingress tests (IngressRecoveryTesting.cpp, GatewayTesting.cpp).
//
// Core has no protocol or product command type of its own, so the tests use a
// minimal stand-in command (TestCommand) with the real gateway, sequence store,
// retransmit server and multicast receiver.

#include <gtest/gtest.h>
#include "MulticastIngressReceiver.hpp"
#include "SequencedInboundMessage.hpp"
#include "NetworkConfig.hpp"
#include "SPSCQueue.hpp"
#include "Alias.hpp"

#include <chrono>
#include <thread>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace ingress_test {

struct TestCommand {
    TraceId trace_id;     // MulticastIngressReceiver reads this one
    SessionId sessionId;  // NetworkGateway fills this one
    uint64_t payload;     // derived from the sequence number, so order can be checked
};
using TestMessage = SequencedInboundMessage<TestCommand>;

constexpr auto GROUP_JOIN_WAIT = std::chrono::milliseconds(100);
constexpr auto NOTHING_ARRIVES_WAIT = std::chrono::milliseconds(300);
constexpr auto TWO_IDLE_TIMEOUTS = std::chrono::milliseconds(700); // receiver idle timeout is 200 ms
constexpr uint32_t RETRANSMIT_BATCH = 500; // must match RetransmitServer::MAX_BATCH

inline uint64_t PayloadFor(uint64_t seq) { return seq * 7 + 3; }

inline TestMessage MakeMessage(uint64_t seq) {
    TestMessage msg{};
    msg.sequenceNumber = seq;
    msg.ingressTaiNs = 1;
    msg.clientSessionId = 1;
    msg.command.trace_id = static_cast<TraceId>(seq);
    msg.command.payload = PayloadFor(seq);
    return msg;
}

inline NetworkConfig MakeConfig(const char* group, uint16_t multicastPort, uint16_t retransmitPort, bool idleProbe) {
    NetworkConfig config;
    config.multicastIp = group;
    config.multicastPort = multicastPort;
    config.retransmitServerIp = "127.0.0.1";
    config.retransmitServerPort = retransmitPort;
    config.ingressIdleProbe = idleProbe;
    return config;
}

// One multicast datagram with exactly these bytes.
inline void SendRaw(const NetworkConfig& config, const void* bytes, size_t length) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    ASSERT_GE(fd, 0);
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(config.multicastPort);
    inet_pton(AF_INET, config.multicastIp.c_str(), &dest.sin_addr);
    ASSERT_EQ(static_cast<ssize_t>(length), sendto(fd, bytes, length, 0, (sockaddr*)&dest, sizeof(dest)));
    close(fd);
}

// Stands in for NetworkGateway's fan-out: ONE multicast datagram that carries
// every message in `msgs`, back to back. Call it only after the messages are
// already in the store, as the gateway does.
inline void SendDatagram(const NetworkConfig& config, const std::vector<TestMessage>& msgs) {
    SendRaw(config, msgs.data(), msgs.size() * sizeof(TestMessage));
}

// Messages for sequence numbers first .. last-1.
inline std::vector<TestMessage> Messages(uint64_t first, uint64_t last) {
    std::vector<TestMessage> msgs;
    for (uint64_t seq = first; seq < last; ++seq) msgs.push_back(MakeMessage(seq));
    return msgs;
}

inline void SendLive(const NetworkConfig& config, const TestMessage& msg) {
    SendDatagram(config, { msg });
}

inline bool PopWithin(SPSCQueue<TestCommand>& queue, TestCommand& out, std::chrono::milliseconds timeout) {
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
inline std::vector<TestCommand> Drain(SPSCQueue<TestCommand>& queue, size_t count,
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
inline void ExpectInOrder(const std::vector<TestCommand>& got, uint64_t count, uint64_t first = 0) {
    ASSERT_EQ(count, got.size()) << "wrong number of commands delivered";
    for (uint64_t i = 0; i < count; ++i) {
        if (got[i].payload != PayloadFor(first + i)) {
            ADD_FAILURE() << "first out-of-order or duplicated command at position " << i
                << ": expected payload " << PayloadFor(first + i) << ", got " << got[i].payload;
            return;
        }
    }
}

inline bool WaitForUnrecoverableGap(const MulticastIngressReceiver<TestCommand>& receiver) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!receiver.hitUnrecoverableGap() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return receiver.hitUnrecoverableGap();
}

} // namespace ingress_test
