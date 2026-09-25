#pragma once
#include "Aggregator.hpp"
#include "TelemetryReport.hpp"

#include <atomic>
#include <thread>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

// Runs inside PerformanceServiceApp: binds a unicast UDP port and hands each
// received TelemetryReport straight to the Aggregator. Unicast (not
// multicast, unlike EgressGateway) — every MatchingService replica's
// TelemetryForwarder sends directly here, and there's exactly one receiver.
// Only takes a port, not an IP, same minimal-constructor shape as
// ReplicaArbiter/RetransmitServer — always binds INADDR_ANY.
class TelemetryReceiver {
public:
    TelemetryReceiver(Aggregator& aggregator, uint16_t listenPort)
        : m_aggregator(aggregator), m_listenPort(listenPort) {
    }

    ~TelemetryReceiver() { stop(); }

    void start() {
        if (m_running.load()) return;

        m_rxFd = socket(AF_INET, SOCK_DGRAM, 0);
        int opt = 1;
        setsockopt(m_rxFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        timeval rxTimeout{};
        rxTimeout.tv_sec = 0;
        rxTimeout.tv_usec = 200'000; // 200ms, so stop() doesn't block on recv()
        setsockopt(m_rxFd, SOL_SOCKET, SO_RCVTIMEO, &rxTimeout, sizeof(rxTimeout));

        sockaddr_in bindAddr{};
        bindAddr.sin_family = AF_INET;
        bindAddr.sin_port = htons(m_listenPort);
        bindAddr.sin_addr.s_addr = INADDR_ANY;
        bind(m_rxFd, (struct sockaddr*)&bindAddr, sizeof(bindAddr));

        m_running.store(true);
        m_worker = std::thread(&TelemetryReceiver::rxLoop, this);
    }

    void stop() {
        if (!m_running.load()) return;
        m_running.store(false);
        if (m_worker.joinable()) m_worker.join();
        if (m_rxFd != -1) close(m_rxFd);
        m_rxFd = -1;
    }

private:
    void rxLoop() {
        while (m_running.load(std::memory_order_relaxed)) {
            TelemetryReport report{};
            ssize_t len = recv(m_rxFd, &report, sizeof(report), 0);
            if (len != static_cast<ssize_t>(sizeof(report))) continue;

            m_aggregator.record(report);
        }
    }

    Aggregator& m_aggregator;
    uint16_t m_listenPort;
    std::atomic<bool> m_running{ false };
    std::thread m_worker;
    int m_rxFd{ -1 };
};
