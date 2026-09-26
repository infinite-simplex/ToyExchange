#pragma once
#include "NetworkConfig.hpp"
#include "SPSCQueue.hpp"
#include "TelemetryReport.hpp"

#include <atomic>
#include <cstring>
#include <thread>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

// Runs inside MatchingServiceApp: drains this replica's own traceQueue
// (SPSCProducerPolicy's telemetry output — previously unconsumed) and
// unicasts each completed OrderTrace, wrapped in a TelemetryReport, to
// PerformanceServiceApp at NetworkConfig::performanceServiceIp/Port.
//
// Deliberately unconditional, unlike EgressPublisher: every replica forwards
// its own telemetry regardless of leadership, since the whole point is
// visibility into ALL replicas' engine performance, not just the current
// leader's. Point-to-point unicast (not multicast) since there is exactly
// one intended receiver.
//
// Sends the raw OrderTrace unmodified — engine-execution/network/queuing
// delta computation happens once, at PerformanceService's Aggregator, not
// duplicated here.
template <size_t Capacity>
class TelemetryForwarder {
public:
    TelemetryForwarder(SPSCQueue<OrderTrace, Capacity>& sourceQueue,
                        const NetworkConfig& config, ReplicaId myId)
        : m_sourceQueue(sourceQueue), m_config(config), m_myId(myId) {
    }

    ~TelemetryForwarder() { stop(); }

    void start() {
        if (m_running.load()) return;

        m_udpFd = socket(AF_INET, SOCK_DGRAM, 0);

        std::memset(&m_destAddr, 0, sizeof(m_destAddr));
        m_destAddr.sin_family = AF_INET;
        m_destAddr.sin_port = htons(m_config.performanceServicePort);
        inet_pton(AF_INET, m_config.performanceServiceIp.c_str(), &m_destAddr.sin_addr);

        m_running.store(true);
        m_worker = std::thread(&TelemetryForwarder::forwardLoop, this);
    }

    void stop() {
        if (!m_running.load()) return;
        m_running.store(false);
        if (m_worker.joinable()) m_worker.join();
        if (m_udpFd != -1) close(m_udpFd);
        m_udpFd = -1;
    }

private:
    void forwardLoop() {
        while (m_running.load(std::memory_order_relaxed)) {
            OrderTrace trace;
            if (!m_sourceQueue.try_pop(trace)) {
                std::this_thread::yield();
                continue;
            }
            TelemetryReport report{ m_myId, trace };
            // Fire-and-forget, same as EgressPublisher: return value not
            // checked, telemetry is already best-effort by the time it gets
            // here (SPSCProducerPolicy::on_trace_complete already drops on a
            // full queue rather than blocking the matching thread).
            sendto(m_udpFd, &report, sizeof(report), 0,
                (struct sockaddr*)&m_destAddr, sizeof(m_destAddr));
        }
    }

    SPSCQueue<OrderTrace, Capacity>& m_sourceQueue;
    NetworkConfig m_config;
    ReplicaId m_myId;
    std::atomic<bool> m_running{ false };
    std::thread m_worker;
    int m_udpFd{ -1 };
    sockaddr_in m_destAddr{};
};
