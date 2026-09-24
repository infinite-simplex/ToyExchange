#pragma once
#include "HeartbeatMessage.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <unordered_map>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

// Runs inside NetworkServiceApp, alongside (not inside) NetworkGateway —
// arbitrates which MatchingService replica is currently allowed to publish
// egress. See EgressGateway's use of currentEpoch()/currentLeader() for the
// other half of the fencing scheme, and LeaderHeartbeatClient for the
// replica side.
//
// Single-threaded request/reply, same shape as RetransmitServer::serveLoop:
// one UDP socket, recvfrom -> update sender's liveness -> recompute leader
// -> reply. Recompute happens reactively, only when a heartbeat arrives,
// not on a separate timer — sufficient with 2+ replicas, since a live
// replica's own next heartbeat is what triggers noticing a stale leader.
class ReplicaArbiter {
public:
    ReplicaArbiter(uint16_t listenPort,
                   std::chrono::milliseconds staleAfter = std::chrono::milliseconds(10),
                   uint64_t lagTolerance = 200)
        : m_listenPort(listenPort), m_staleAfter(staleAfter), m_lagTolerance(lagTolerance) {
    }

    ~ReplicaArbiter() { stop(); }

    void start() {
        if (m_running.load()) return;

        m_socket = socket(AF_INET, SOCK_DGRAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(m_listenPort);
        bind(m_socket, (sockaddr*)&addr, sizeof(addr));

        m_running.store(true);
        m_thread = std::thread(&ReplicaArbiter::serveLoop, this);
    }

    void stop() {
        if (!m_running.load()) return;
        m_running.store(false);
        if (m_socket != -1) { shutdown(m_socket, SHUT_RDWR); close(m_socket); }
        if (m_thread.joinable()) m_thread.join();
        m_socket = -1;
    }

    // Safe to call from another thread (EgressGateway's rxLoop) — serveLoop
    // is the sole writer, so a plain atomic load is enough; no mutex needed.
    FencingEpoch currentEpoch() const { return m_epoch.load(std::memory_order_acquire); }
    ReplicaId currentLeader() const { return m_leaderId.load(std::memory_order_acquire); }

private:
    struct ReplicaStatus {
        std::chrono::steady_clock::time_point lastSeen;
        uint64_t lastAppliedSeq{ 0 };
    };

    void serveLoop() {
        while (m_running.load(std::memory_order_relaxed)) {
            HeartbeatRequest req{};
            sockaddr_in fromAddr{};
            socklen_t fromLen = sizeof(fromAddr);
            ssize_t n = recvfrom(m_socket, &req, sizeof(req), 0, (sockaddr*)&fromAddr, &fromLen);
            if (n != sizeof(req)) continue; // malformed, or unblocked by stop()'s shutdown() — ignore

            auto now = std::chrono::steady_clock::now();
            m_replicas[req.replicaId] = ReplicaStatus{ now, req.lastAppliedSeq };

            recomputeLeader(now);

            HeartbeatResponse resp{ currentEpoch(), currentLeader() };
            sendto(m_socket, &resp, sizeof(resp), 0, (sockaddr*)&fromAddr, fromLen);
        }
    }

    void recomputeLeader(std::chrono::steady_clock::time_point now) {
        uint64_t maxSeq = 0;
        for (const auto& [id, status] : m_replicas) {
            maxSeq = std::max(maxSeq, status.lastAppliedSeq);
        }

        auto isEligible = [&](const ReplicaStatus& status) {
            bool fresh = (now - status.lastSeen) <= m_staleAfter;
            bool caughtUp = status.lastAppliedSeq + m_lagTolerance >= maxSeq;
            return fresh && caughtUp;
        };

        ReplicaId current = currentLeader();
        auto currentIt = m_replicas.find(current);
        if (current != INVALID_REPLICA_ID && currentIt != m_replicas.end() && isEligible(currentIt->second)) {
            return; // sticky — current leader is still good, no change
        }

        ReplicaId newLeader = INVALID_REPLICA_ID;
        for (const auto& [id, status] : m_replicas) {
            if (!isEligible(status)) continue;
            if (newLeader == INVALID_REPLICA_ID || id < newLeader) newLeader = id;
        }

        // Bump on ANY change, including to/from "nobody" — see FencingEpoch
        // doc comment in Alias.hpp for why this matters even for that case.
        if (newLeader != current) {
            m_leaderId.store(newLeader, std::memory_order_release);
            m_epoch.fetch_add(1, std::memory_order_acq_rel);
        }
    }

    uint16_t m_listenPort;
    std::chrono::milliseconds m_staleAfter;
    uint64_t m_lagTolerance;

    int m_socket = -1;
    std::atomic<bool> m_running{ false };
    std::thread m_thread;

    // Single-writer (serveLoop thread only) — no mutex needed.
    std::unordered_map<ReplicaId, ReplicaStatus> m_replicas;
    std::atomic<FencingEpoch> m_epoch{ NO_EPOCH };
    std::atomic<ReplicaId> m_leaderId{ INVALID_REPLICA_ID };
};
