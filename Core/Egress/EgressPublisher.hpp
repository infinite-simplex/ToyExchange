#pragma once
#include "EgressBatch.hpp"
#include "SPSCQueue.hpp"
#include "NetworkConfig.hpp"

#include <atomic>
#include <cstring>
#include <thread>
#include <type_traits>
#include <vector>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

// Runs inside MatchingServiceApp: drains the matching engine's own output
// queue (SPSCProducerPolicy's event queue) and multicasts the events to
// NetworkConfig::egressMulticastIp/Port, where every NetworkServiceApp's
// EgressGateway picks them up to derive the private ack and public broadcast.
// Deliberately simple for now: no gap-fill/retransmit on this channel (unlike
// the ingress replication channel). Only the replica the heartbeat client
// currently believes is leader actually sends — see LeaderHeartbeatClient —
// so with more than one active replica, exactly one of them publishes at a
// time instead of every replica publishing independently.
//
// Batching: each pass takes every event the queue holds right then, up to one
// datagram's worth (NetworkConfig::egressDatagramMaxBytes), and sends them as
// ONE datagram behind an EgressBatchHeader (see EgressBatch.hpp). It never
// waits for more events, so an event that is alone goes out alone, at once.
// One datagram per event cost the leader a system call each, which at about
// 70,000 orders/s is more than a replica can afford: its event queue filled,
// its matching thread blocked, and the arbiter, correctly, demoted the lagging
// leader again and again.
template <typename T, size_t Capacity, typename THeartbeatClient>
class EgressPublisher {
    static_assert(std::is_trivially_copyable_v<T>, "events are sent as raw bytes");

public:
    EgressPublisher(SPSCQueue<T, Capacity>& sourceQueue, const NetworkConfig& config,
                     const THeartbeatClient& heartbeatClient)
        : m_sourceQueue(sourceQueue), m_config(config), m_heartbeatClient(heartbeatClient),
          m_eventsPerDatagram(EventsPerEgressDatagram<T>(config.egressDatagramMaxBytes)),
          m_packet(sizeof(EgressBatchHeader) + m_eventsPerDatagram * sizeof(T)) {
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

    // Events taken from the queue so far, published or not. This is the next event's ordinal.
    uint64_t eventsConsumed() const { return m_eventsConsumed.load(std::memory_order_relaxed); }

private:
    void publishLoop() {
        uint64_t ordinal = 0; // position of the next event in this replica's event stream
        char* const events = m_packet.data() + sizeof(EgressBatchHeader);

        while (m_running.load(std::memory_order_relaxed)) {
            size_t count = 0;
            T item;
            while (count < m_eventsPerDatagram && m_sourceQueue.try_pop(item)) {
                std::memcpy(events + count * sizeof(T), &item, sizeof(T));
                ++count;
            }
            if (count == 0) {
                std::this_thread::yield();
                continue;
            }

            // Always drain the queue regardless of leadership, so a passive
            // replica's matching thread never backs up waiting on this one —
            // only the actual sendto() is conditional. Every event is numbered,
            // sent or not, so the numbering matches on every replica.
            if (m_heartbeatClient.isLeader()) {
                const EgressBatchHeader header{
                    m_heartbeatClient.currentEpoch(), ordinal, m_heartbeatClient.myId(),
                    static_cast<uint16_t>(count), 0 };
                std::memcpy(m_packet.data(), &header, sizeof(header));
                sendto(m_udpFd, m_packet.data(), sizeof(header) + count * sizeof(T), 0,
                    (struct sockaddr*)&m_destAddr, sizeof(m_destAddr));
            }
            ordinal += count;
            m_eventsConsumed.store(ordinal, std::memory_order_relaxed);
        }
    }

    SPSCQueue<T, Capacity>& m_sourceQueue;
    NetworkConfig m_config;
    const THeartbeatClient& m_heartbeatClient;
    size_t m_eventsPerDatagram;
    std::vector<char> m_packet; // one datagram, reused; publisher thread only
    std::atomic<bool> m_running{ false };
    std::atomic<uint64_t> m_eventsConsumed{ 0 };
    std::thread m_worker;
    int m_udpFd{ -1 };
    sockaddr_in m_destAddr{};
};
