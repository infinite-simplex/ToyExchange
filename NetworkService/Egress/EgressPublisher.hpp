#pragma once
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
// queue (SPSCProducerPolicy's event queue) and multicasts each raw T (e.g.
// OrderEvent) to NetworkConfig::egressMulticastIp/Port, where every
// NetworkServiceApp's EgressGateway picks it up to derive the private OUCH
// ack and public ITCH broadcast. Deliberately simple for now: no gap-fill/
// retransmit on this channel (unlike the ingress replication channel), and
// every replica publishes independently — with more than one active replica
// the same event is published once per replica until leader election exists
// (m_isLeader below is the seam for that, currently hardcoded on).
template <typename T, size_t Capacity = 1024>
class EgressPublisher {
public:
    EgressPublisher(SPSCQueue<T, Capacity>& sourceQueue, const NetworkConfig& config)
        : m_sourceQueue(sourceQueue), m_config(config) {
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
        // TODO: gate on leader status once replica leader election exists —
        // a passive replica should still drain its queue (so it doesn't
        // back up/stall the matching thread) but skip the actual sendto().
        while (m_running.load(std::memory_order_relaxed)) {
            T item;
            if (!m_sourceQueue.try_pop(item)) {
                std::this_thread::yield();
                continue;
            }
            if (m_isLeader) {
                sendto(m_udpFd, &item, sizeof(item), 0,
                    (struct sockaddr*)&m_destAddr, sizeof(m_destAddr));
            }
        }
    }

    SPSCQueue<T, Capacity>& m_sourceQueue;
    NetworkConfig m_config;
    std::atomic<bool> m_running{ false };
    std::thread m_worker;
    int m_udpFd{ -1 };
    sockaddr_in m_destAddr{};
    bool m_isLeader{ true }; // stub — always publish until leader election exists
};
