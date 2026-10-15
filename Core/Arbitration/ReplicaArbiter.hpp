#pragma once
#include "HeartbeatMessage.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
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
//
// A replica is eligible to lead when it is fresh (it heartbeated within
// staleAfter) and not behind. "Behind" is measured in time, not in commands:
// a replica is behind when it has not reached the highest sequence number
// any replica had reported lagWindow ago. A count of commands cannot work at
// every rate. Replicas heartbeat at their own moments, so two replicas that
// are fully caught up still report sequence numbers that differ by the
// command rate times the heartbeat interval. At 500,000 commands per second
// and a 2 ms interval that is about 1,000 commands.
class ReplicaArbiter {
public:
    ReplicaArbiter(uint16_t listenPort,
                   std::chrono::milliseconds staleAfter = std::chrono::milliseconds(10),
                   std::chrono::milliseconds lagWindow = std::chrono::milliseconds(20))
        : m_listenPort(listenPort), m_staleAfter(staleAfter), m_lagWindow(lagWindow) {
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

        // lagFloor is the highest sequence number any replica had reported
        // lagWindow ago: the latest sample at or before that moment. Samples
        // older than that one are no longer needed. Until the arbiter has run
        // for a full window there is no such sample, and no replica is behind.
        m_maxSeqHistory.push_back({ now, maxSeq });
        const auto cutoff = now - m_lagWindow;
        while (m_maxSeqHistory.size() > 1 && m_maxSeqHistory[1].when <= cutoff) {
            m_maxSeqHistory.pop_front();
        }
        const uint64_t lagFloor = (m_maxSeqHistory.front().when <= cutoff) ? m_maxSeqHistory.front().maxSeq : 0;

        auto isEligible = [&](const ReplicaStatus& status) {
            bool fresh = (now - status.lastSeen) <= m_staleAfter;
            bool caughtUp = status.lastAppliedSeq >= lagFloor;
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
    std::chrono::milliseconds m_lagWindow;

    // The highest lastAppliedSeq across replicas, sampled at every
    // recompute, oldest first. Single-writer (serveLoop thread only).
    struct MaxSeqSample {
        std::chrono::steady_clock::time_point when;
        uint64_t maxSeq;
    };
    std::deque<MaxSeqSample> m_maxSeqHistory;

    int m_socket = -1;
    std::atomic<bool> m_running{ false };
    std::thread m_thread;

    // Single-writer (serveLoop thread only) — no mutex needed.
    std::unordered_map<ReplicaId, ReplicaStatus> m_replicas;
    std::atomic<FencingEpoch> m_epoch{ NO_EPOCH };
    std::atomic<ReplicaId> m_leaderId{ INVALID_REPLICA_ID };
};
