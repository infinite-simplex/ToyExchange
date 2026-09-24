#pragma once
#include "HeartbeatMessage.hpp"
#include "NetworkConfig.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

// Runs inside MatchingServiceApp: sends this replica's liveness (its own id
// plus how far it's gotten through the ingress sequence) to the gateway's
// ReplicaArbiter roughly every HEARTBEAT_INTERVAL, and remembers the
// {epoch, leaderId} it's told in return. EgressPublisher reads isLeader()/
// currentEpoch() from here rather than deciding for itself.
//
// TMatchingService is templated (rather than including MatchingService.hpp
// directly) so NetworkService doesn't need a hard dependency on
// MatchingService's types — same reasoning as EgressGateway's TGateway
// parameter. Only requirement: a const uint64_t lastAppliedSeq() method.
template <typename TMatchingService>
class LeaderHeartbeatClient {
public:
    LeaderHeartbeatClient(ReplicaId myId, const TMatchingService& matcher, const NetworkConfig& config)
        : m_myId(myId), m_matcher(matcher), m_config(config) {
    }

    ~LeaderHeartbeatClient() { stop(); }

    void start() {
        if (m_running.load()) return;

        m_socket = socket(AF_INET, SOCK_DGRAM, 0);
        timeval rxTimeout{};
        rxTimeout.tv_sec = 0;
        rxTimeout.tv_usec = 50'000; // 50ms — well under the send interval, so a missed reply never stalls the next send
        setsockopt(m_socket, SOL_SOCKET, SO_RCVTIMEO, &rxTimeout, sizeof(rxTimeout));

        std::memset(&m_gatewayAddr, 0, sizeof(m_gatewayAddr));
        m_gatewayAddr.sin_family = AF_INET;
        m_gatewayAddr.sin_port = htons(m_config.arbitrationPort);
        // Reuses retransmitServerIp — from a replica's point of view this
        // is simply "the gateway's address," already used for gap-fill.
        inet_pton(AF_INET, m_config.retransmitServerIp.c_str(), &m_gatewayAddr.sin_addr);

        m_running.store(true);
        m_worker = std::thread(&LeaderHeartbeatClient::heartbeatLoop, this);
    }

    void stop() {
        if (!m_running.load()) return;
        m_running.store(false);
        if (m_worker.joinable()) m_worker.join();
        if (m_socket != -1) close(m_socket);
        m_socket = -1;
    }

    bool isLeader() const { return m_leaderId.load(std::memory_order_acquire) == m_myId; }
    FencingEpoch currentEpoch() const { return m_epoch.load(std::memory_order_acquire); }
    ReplicaId myId() const { return m_myId; }

private:
    static constexpr auto HEARTBEAT_INTERVAL = std::chrono::milliseconds(2);

    void heartbeatLoop() {
        while (m_running.load(std::memory_order_relaxed)) {
            HeartbeatRequest req{ m_myId, m_matcher.lastAppliedSeq() };
            sendto(m_socket, &req, sizeof(req), 0, (sockaddr*)&m_gatewayAddr, sizeof(m_gatewayAddr));

            HeartbeatResponse resp{};
            ssize_t n = recv(m_socket, &resp, sizeof(resp), 0);
            if (n == sizeof(resp)) {
                m_epoch.store(resp.epoch, std::memory_order_release);
                m_leaderId.store(resp.leaderId, std::memory_order_release);
            }
            // A missed/timed-out reply just means we retry next interval;
            // isLeader()/currentEpoch() keep returning the last known-good
            // values, which safely defaults to "not leader" until we're
            // ever told otherwise.

            std::this_thread::sleep_for(HEARTBEAT_INTERVAL);
        }
    }

    ReplicaId m_myId;
    const TMatchingService& m_matcher;
    NetworkConfig m_config;

    int m_socket{ -1 };
    std::atomic<bool> m_running{ false };
    std::thread m_worker;
    sockaddr_in m_gatewayAddr{};

    std::atomic<FencingEpoch> m_epoch{ NO_EPOCH };
    std::atomic<ReplicaId> m_leaderId{ INVALID_REPLICA_ID };
};
