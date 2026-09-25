#pragma once
#include "SPSCQueue.hpp"
#include "SequencedInboundMessage.hpp"
#include "RetransmitMessage.hpp"
#include "NetworkConfig.hpp"
#include "Telemetry.hpp"

#include <atomic>
#include <thread>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>

// Consumer side of NetworkGateway<TProtocolHandler, TCommand>'s UDP multicast
// fan-out: receives the same SequencedInboundMessage<TCommand> wire struct
// NetworkGateway builds and sends, detects sequence gaps, recovers them via
// RetransmitServer<TCommand> (see RetransmitProtocol.hpp), and pushes each
// TCommand into the local Orderbook Input Disruptor (SPSCQueue<TCommand>).
template <typename TCommand>
class MulticastIngressReceiver {
public:
    MulticastIngressReceiver(SPSCQueue<TCommand>& inboundQueue, const NetworkConfig& config)
        : m_inboundQueue(inboundQueue), m_config(config) {
    }

    ~MulticastIngressReceiver() { stop(); }

    void start() {
        if (m_running.load()) return;

        m_udpFd = socket(AF_INET, SOCK_DGRAM, 0);

        int opt = 1;
        setsockopt(m_udpFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        // Bounded timeout so rxLoop wakes up periodically to re-check m_running
        // instead of relying on a concurrent close() from stop() to unblock recv().
        timeval rxTimeout{};
        rxTimeout.tv_sec = 0;
        rxTimeout.tv_usec = 200'000; // 200ms
        setsockopt(m_udpFd, SOL_SOCKET, SO_RCVTIMEO, &rxTimeout, sizeof(rxTimeout));

        sockaddr_in bindAddr{};
        bindAddr.sin_family = AF_INET;
        bindAddr.sin_port = htons(m_config.multicastPort);
        bindAddr.sin_addr.s_addr = INADDR_ANY;
        bind(m_udpFd, (struct sockaddr*)&bindAddr, sizeof(bindAddr));

        ip_mreq mreq{};
        inet_pton(AF_INET, m_config.multicastIp.c_str(), &mreq.imr_multiaddr);
        mreq.imr_interface.s_addr = INADDR_ANY;
        setsockopt(m_udpFd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));

        m_running.store(true);
        m_worker = std::thread(&MulticastIngressReceiver::rxLoop, this);
    }

    void stop() {
        if (!m_running.load()) return;
        m_running.store(false);

        if (m_udpFd != -1) close(m_udpFd);
        if (m_worker.joinable()) m_worker.join();
        m_udpFd = -1;
    }

    // True once rxLoop has given up on a gap it can never recover from (see
    // rxLoop's FATAL branch). Polled by main() alongside its own shutdown
    // flag — this replica cannot be trusted to keep running.
    bool hitUnrecoverableGap() const { return m_hitUnrecoverableGap.load(std::memory_order_relaxed); }

private:
    using Message = SequencedInboundMessage<TCommand>;

    void rxLoop() {
        uint64_t expectedSeq = 0; // matches NetworkGateway::m_sequenceNumber, which starts at 0

        while (m_running.load(std::memory_order_relaxed)) {
            Message msg{};
            ssize_t len = recv(m_udpFd, &msg, sizeof(msg), 0);
            if (len < static_cast<ssize_t>(sizeof(Message))) {
                continue; // Fragmented or faulty packet drop
            }

            // Sequence gap checking
            if (msg.sequenceNumber != expectedSeq) {
                std::cerr << "[WARNING] Sequence gap detected! Expected: "
                    << expectedSeq << " Got: " << msg.sequenceNumber << "\n";

                if (msg.sequenceNumber > expectedSeq) {
                    uint64_t gapCount = msg.sequenceNumber - expectedSeq;
                    if (!requestRetransmit(expectedSeq, gapCount)) {
                        // Unrecoverable: SequenceStore is a ring buffer that only
                        // moves forward, so once a range is evicted it is gone for
                        // good — no future retry, restart included, can ever fill
                        // it. This replica's book is now permanently incomplete
                        // and cannot be trusted with anything further. Stop rather
                        // than silently continuing on known-wrong state; main()
                        // polls hitUnrecoverableGap() and shuts the process down,
                        // and ReplicaArbiter's existing heartbeat-staleness check
                        // excludes a gone replica from leadership exactly like a
                        // crash would — no separate signaling needed.
                        std::cerr << "[FATAL] Unrecoverable sequence gap [" << expectedSeq << ", "
                            << msg.sequenceNumber << "); this replica's book is permanently "
                            << "incomplete, stopping.\n";
                        m_hitUnrecoverableGap.store(true, std::memory_order_relaxed);
                        return;
                    }
                }
                expectedSeq = msg.sequenceNumber;
            }
            expectedSeq++;

            pushWithTelemetry(msg);
        }
    }

    // This is the "Ingress Server Thread" from OrderTrace's point of view for
    // this process: stamps the ingress_tai_ns NetworkGateway captured at
    // socket read (carried over the wire in the message, as a synced
    // wall-clock reading since it's read back on a different machine — see
    // get_synced_time_ns() in Telemetry.hpp), times the enqueue itself with
    // cheap local rdtsc (self-consistent, both reads on this machine), and
    // records the resulting queue depth — then pushes into the local
    // Orderbook Input Disruptor (SPSC Queue). Used for both live traffic and
    // gap-filled records recovered via requestRetransmit, so recovered orders
    // get the same telemetry as ones that arrived live.
    // Returns false only if shutdown was requested mid-push (queue stayed full).
    bool pushWithTelemetry(const Message& msg) {
        OrderTrace& trace = g_telemetry_arena[trace_index(msg.command.trace_id)];

        // This is the sole slot-claim site: if the slot's previous occupant
        // never got a chance to release it (complete_trace() below) before
        // wrapping all the way back around to us, that's a genuine
        // wraparound collision — most likely a long-resting GTC order whose
        // telemetry is about to be silently overwritten. Not preventable
        // without unbounded memory; at least make it observable.
        if (trace.owner_trace_id != INVALID_TRACE_ID && trace.owner_trace_id != msg.command.trace_id) {
            g_telemetry_arena_collisions.fetch_add(1, std::memory_order_relaxed);
        }
        trace.owner_trace_id = msg.command.trace_id;

        trace.ingress_tai_ns = msg.ingressTaiNs;
        trace.replica_ingress_tai_ns = get_synced_time_ns();

        uint64_t start = get_time();
        while (!m_inboundQueue.try_push(msg.command)) {
            if (!m_running.load(std::memory_order_relaxed)) return false;
            std::this_thread::yield();
        }
        trace.inbound_q_submitted = get_time() - start;
        trace.inbound_q_depth = static_cast<uint32_t>(m_inboundQueue.size());
        return true;
    }

    // Requests [startSeq, startSeq + count) from RetransmitServer<TCommand> and
    // pushes recovered records into the queue in order. Returns false if the
    // server didn't fully satisfy the request (unreachable, too old, not yet
    // arrived) — the caller resyncs to live traffic in that case rather than
    // blocking the consumer thread indefinitely.
    bool requestRetransmit(uint64_t startSeq, uint64_t count) {
        static constexpr uint32_t MAX_BATCH = 500; // must match RetransmitServer::MAX_BATCH
        static constexpr int MAX_ATTEMPTS = 3;

        int sock = socket(AF_INET, SOCK_DGRAM, 0);
        if (sock < 0) return false;

        timeval timeout{};
        timeout.tv_sec = 0;
        timeout.tv_usec = 200'000; // 200ms
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        sockaddr_in serverAddr{};
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(m_config.retransmitServerPort);
        inet_pton(AF_INET, m_config.retransmitServerIp.c_str(), &serverAddr.sin_addr);

        bool fullyRecovered = true;
        uint64_t seq = startSeq;
        uint64_t remaining = count;

        while (remaining > 0) {
            uint32_t batch = static_cast<uint32_t>(remaining > MAX_BATCH ? MAX_BATCH : remaining);
            RetransmitRequest req{ seq, batch };

            bool batchOk = false;
            for (int attempt = 0; attempt < MAX_ATTEMPTS && !batchOk; ++attempt) {
                if (sendto(sock, &req, sizeof(req), 0,
                    (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
                    continue;
                }

                uint32_t received = 0;
                while (received < batch) {
                    char packet[sizeof(RetransmitRecordHeader) + sizeof(Message)];
                    ssize_t n = recv(sock, packet, sizeof(packet), 0);
                    if (n < static_cast<ssize_t>(sizeof(RetransmitRecordHeader))) {
                        break; // timeout or malformed response — retry the batch
                    }

                    RetransmitRecordHeader hdr{};
                    std::memcpy(&hdr, packet, sizeof(hdr));

                    if (hdr.status != RetransmitStatus::OK) {
                        break; // rest of the batch is equally unavailable
                    }
                    if (n != static_cast<ssize_t>(sizeof(packet))) {
                        break; // truncated record — retry the batch
                    }

                    Message record{};
                    std::memcpy(&record, packet + sizeof(hdr), sizeof(record));

                    if (!pushWithTelemetry(record)) {
                        close(sock);
                        return false;
                    }

                    ++seq;
                    ++received;
                }

                batchOk = (received == batch);
            }

            if (!batchOk) {
                fullyRecovered = false;
                break;
            }
            remaining -= batch;
        }

        close(sock);
        return fullyRecovered;
    }

    SPSCQueue<TCommand>& m_inboundQueue;
    NetworkConfig m_config;
    std::atomic<bool> m_running{ false };
    std::thread m_worker;
    int m_udpFd{ -1 };
    std::atomic<bool> m_hitUnrecoverableGap{ false };
};
