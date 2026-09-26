#pragma once
#include "FencedMessage.hpp"
#include "SPSCQueue.hpp"
#include "NetworkConfig.hpp"

#include <atomic>
#include <cstring>
#include <thread>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

// Runs inside MatchingServiceApp: drains the matching engine's own output
// queue (SPSCProducerPolicy's event queue) and multicasts each item, wrapped
// in a FencedMessage<T>, to NetworkConfig::egressMulticastIp/Port, where
// every NetworkServiceApp's EgressGateway picks it up to derive the private
// OUCH ack and public ITCH broadcast. Deliberately simple for now: no
// gap-fill/retransmit on this channel (unlike the ingress replication
// channel). Only the replica the heartbeat client currently believes is
// leader actually sends — see LeaderHeartbeatClient — so with more than one
// active replica, exactly one of them publishes at a time instead of every
// replica publishing independently.
template <typename T, size_t Capacity, typename THeartbeatClient>
class EgressPublisher {
public:
    EgressPublisher(SPSCQueue<T, Capacity>& sourceQueue, const NetworkConfig& config,
                     const THeartbeatClient& heartbeatClient)
        : m_sourceQueue(sourceQueue), m_config(config), m_heartbeatClient(heartbeatClient) {
    }

    ~EgressPublisher() { stop(); }

    void start() {
        if (m_running.load()) return;

        m_udpFd = socket(AF_INET, SOCK_DGRAM, 0);
        unsigned char ttl = 1;
        setsockopt(m_udpFd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));

        std::memset(&m_destAddr, 0, sizeof(m_destAddr));
        m_destAddr.sin_family = AF_INET;
        m_destAddr.sin_port = htons(m_config.egressMulticastPort);
        inet_pton(AF_INET, m_config.egressMulticastIp.c_str(), &m_destAddr.sin_addr);

        m_running.store(true);
        m_worker = std::thread(&EgressPublisher::publishLoop, this);
    }

    void stop() {
        if (!m_running.load()) return;
        m_running.store(false);
        if (m_worker.joinable()) m_worker.join();
        if (m_udpFd != -1) close(m_udpFd);
        m_udpFd = -1;
    }

private:
    void publishLoop() {
        while (m_running.load(std::memory_order_relaxed)) {
            T item;
            if (!m_sourceQueue.try_pop(item)) {
                std::this_thread::yield();
                continue;
            }
            // Always drain the queue regardless of leadership, so a passive
            // replica's matching thread never backs up waiting on this one —
            // only the actual sendto() is conditional.
            if (m_heartbeatClient.isLeader()) {
                FencedMessage<T> msg{ m_heartbeatClient.currentEpoch(), m_heartbeatClient.myId(), item };
                sendto(m_udpFd, &msg, sizeof(msg), 0,
                    (struct sockaddr*)&m_destAddr, sizeof(m_destAddr));
            }
        }
    }

    SPSCQueue<T, Capacity>& m_sourceQueue;
    NetworkConfig m_config;
    const THeartbeatClient& m_heartbeatClient;
    std::atomic<bool> m_running{ false };
    std::thread m_worker;
    int m_udpFd{ -1 };
    sockaddr_in m_destAddr{};
};
