#pragma once
#include "EgressBatch.hpp"
#include "NetworkConfig.hpp"
#include "ReceiveBuffer.hpp"
#include "ReplicaArbiter.hpp"
#include "SpinReceive.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <sstream>
#include <thread>
#include <type_traits>
#include <vector>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

// What EgressGateway has done with the datagrams it received. Read it from any thread.
struct EgressStats {
    uint64_t batches = 0;             // datagrams accepted
    uint64_t events = 0;              // events delivered to their sessions and the public feed
    uint64_t staleBatches = 0;        // datagrams dropped for an older epoch
    uint64_t staleEvents = 0;         // events in those datagrams
    uint64_t malformedDatagrams = 0;  // too short, or a size that does not match its header
    uint64_t missingEvents = 0;       // event ordinals that never arrived from any replica
    uint64_t repeatedEvents = 0;      // events whose ordinal had already arrived
};

// Runs inside NetworkServiceApp: joins the internal egress multicast channel
// every MatchingServiceApp replica's EgressPublisher writes to, and for each
// EgressBatchHeader datagram (see EgressBatch.hpp) it receives:
//  - drops it if its epoch is stale (see ReplicaArbiter — this is what makes a
//    failed-over replica's late traffic harmless without it needing to notice
//    its own demotion),
//  - for each event, prints it to console when NetworkConfig::egressConsoleEcho
//    is set (a stand-in for a real market-data consumer; one write per batch),
//  - sends the private ack back over the originating client's own TCP
//    connection via TGateway::sendToSession, ONE call per client per batch
//    (a no-op if that client has since disconnected),
//  - and broadcasts the same encoded bytes to the public broadcast group,
//    packed back to back into datagrams of at most egressDatagramMaxBytes. A
//    consumer of the public feed must therefore find frame boundaries itself,
//    which self-delimiting protocols like OUCH and ITCH allow.
//
// Per-event work is only encoding and copying. The system calls are per batch,
// per client and per datagram, never per event.
//
// TGateway is NetworkGateway<TProtocolHandler, TCommand> — templated here so
// this header doesn't need to know either of its own template parameters.
// TEncoder converts a TEvent into wire bytes and formats it for the console
// (static encode()/print() — see e.g. OuchEventEncoder in OrderEventFrame.hpp)
// — this header has no knowledge of any specific product's event format.
// TEvent must be trivially copyable and have a session_id member.
template <typename TGateway, typename TEvent, typename TEncoder>
class EgressGateway {
    static_assert(std::is_trivially_copyable_v<TEvent>, "events arrive as raw bytes");

    // At most one loss report in this time (see reportLoss).
    static constexpr std::chrono::seconds LOSS_REPORT_INTERVAL{ 1 };

public:
    EgressGateway(TGateway& gateway, const NetworkConfig& config, const ReplicaArbiter& arbiter)
        : m_gateway(gateway), m_config(config), m_arbiter(arbiter) {
    }

    ~EgressGateway() { stop(); }

    void start() {
        if (m_running.load()) return;

        m_rxFd = socket(AF_INET, SOCK_DGRAM, 0);
        int opt = 1;
        setsockopt(m_rxFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        m_receiveBufferBytes = SetReceiveBuffer(m_rxFd, m_config.egressReceiveBufferBytes);
        if (m_receiveBufferBytes < m_config.egressReceiveBufferBytes) {
            std::cerr << "[WARNING] Egress receive buffer is " << m_receiveBufferBytes
                << " bytes, below the " << m_config.egressReceiveBufferBytes
                << " requested. Raise net.core.rmem_max on this host, or bursts can drop egress datagrams.\n";
        }

        timeval rxTimeout{};
        rxTimeout.tv_sec = 0;
        rxTimeout.tv_usec = 200'000; // 200ms, so stop() doesn't block on recv()
        setsockopt(m_rxFd, SOL_SOCKET, SO_RCVTIMEO, &rxTimeout, sizeof(rxTimeout));

        sockaddr_in bindAddr{};
        bindAddr.sin_family = AF_INET;
        bindAddr.sin_port = htons(m_config.egressMulticastPort);
        bindAddr.sin_addr.s_addr = INADDR_ANY;
        bind(m_rxFd, (struct sockaddr*)&bindAddr, sizeof(bindAddr));

        ip_mreq mreq{};
        inet_pton(AF_INET, m_config.egressMulticastIp.c_str(), &mreq.imr_multiaddr);
        mreq.imr_interface.s_addr = INADDR_ANY;
        setsockopt(m_rxFd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));

        m_broadcastFd = socket(AF_INET, SOCK_DGRAM, 0);
        unsigned char ttl = 1;
        setsockopt(m_broadcastFd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
        std::memset(&m_broadcastDestAddr, 0, sizeof(m_broadcastDestAddr));
        m_broadcastDestAddr.sin_family = AF_INET;
        m_broadcastDestAddr.sin_port = htons(m_config.publicBroadcastPort);
        inet_pton(AF_INET, m_config.publicBroadcastIp.c_str(), &m_broadcastDestAddr.sin_addr);

        m_running.store(true);
        m_worker = std::thread(&EgressGateway::rxLoop, this);
    }

    void stop() {
        if (!m_running.load()) return;
        m_running.store(false);
        if (m_worker.joinable()) m_worker.join();
        if (m_rxFd != -1) close(m_rxFd);
        if (m_broadcastFd != -1) close(m_broadcastFd);
        m_rxFd = -1;
        m_broadcastFd = -1;
    }

    // Receive buffer the kernel granted, in bytes. Valid after start().
    int receiveBufferBytes() const { return m_receiveBufferBytes; }

    EgressStats stats() const {
        EgressStats s;
        s.batches = m_batches.load(std::memory_order_relaxed);
        s.events = m_events.load(std::memory_order_relaxed);
        s.staleBatches = m_staleBatches.load(std::memory_order_relaxed);
        s.staleEvents = m_staleEvents.load(std::memory_order_relaxed);
        s.malformedDatagrams = m_malformed.load(std::memory_order_relaxed);
        s.missingEvents = m_missingEvents.load(std::memory_order_relaxed);
        s.repeatedEvents = m_repeatedEvents.load(std::memory_order_relaxed);
        return s;
    }

private:
    // The frames of one batch that go to one client, in event order.
    struct SessionOutput {
        SessionId id = 0;
        std::vector<char> bytes;
    };

    void rxLoop() {
        std::vector<char> rx(MAX_UDP_PAYLOAD_BYTES);
        bool spin = false; // only right after traffic; an idle gateway sleeps

        while (m_running.load(std::memory_order_relaxed)) {
            const ssize_t len = ReceiveDatagram(m_rxFd, rx.data(), rx.size(), spin, m_config.egressSpinMicros);
            if (len <= 0) {
                spin = false;
                flushPendingSessions(); // a quiet moment: let a slow client catch up
                reportLoss(false);
                continue;
            }
            spin = true;
            handleDatagram(rx.data(), static_cast<size_t>(len));
            flushPendingSessions();
            reportLoss(false);
        }
        reportLoss(true); // nothing is lost without a word, even in the last moments
    }

    // Says how many events never arrived since the last report. A burst of drops costs one line, not one per
    // datagram: after a report, the next waits for LOSS_REPORT_INTERVAL unless `final` is set.
    void reportLoss(bool final) {
        const uint64_t missing = m_missingEvents.load(std::memory_order_relaxed);
        if (missing == m_reportedMissing) return;
        const auto now = std::chrono::steady_clock::now();
        if (!final && m_reportedAny && now - m_lastLossReport < LOSS_REPORT_INTERVAL) return;
        std::cerr << "[WARNING] Egress gateway did not receive " << (missing - m_reportedMissing) << " events ("
            << missing << " since start). A datagram was dropped, the gateway fell behind, or the leader changed.\n";
        m_reportedMissing = missing;
        m_lastLossReport = now;
        m_reportedAny = true;
    }

    void handleDatagram(const char* data, size_t length) {
        if (length < sizeof(EgressBatchHeader)) {
            m_malformed.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        EgressBatchHeader header;
        std::memcpy(&header, data, sizeof(header));
        if (header.count == 0 || length != sizeof(header) + static_cast<size_t>(header.count) * sizeof(TEvent)) {
            m_malformed.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        // Zero-latency, in-process check — no network round trip needed
        // since the arbiter that decided this epoch lives right here.
        if (header.epoch < m_arbiter.currentEpoch()) { // stale, from a deposed leader
            m_staleBatches.fetch_add(1, std::memory_order_relaxed);
            m_staleEvents.fetch_add(header.count, std::memory_order_relaxed);
            return;
        }

        accountForOrdinals(header);

        const char* events = data + sizeof(header);
        for (uint16_t i = 0; i < header.count; ++i) {
            TEvent evt;
            std::memcpy(&evt, events + static_cast<size_t>(i) * sizeof(TEvent), sizeof(TEvent));
            if (m_config.egressConsoleEcho) {
                m_echo << "[egress] ";
                TEncoder::print(m_echo, evt);
                m_echo << '\n';
            }
            const auto frame = TEncoder::encode(evt);
            std::vector<char>& toClient = sessionOutput(evt.session_id).bytes;
            toClient.insert(toClient.end(), frame.data(), frame.data() + frame.size());
            appendToFeed(frame.data(), frame.size());
        }
        m_batches.fetch_add(1, std::memory_order_relaxed);
        m_events.fetch_add(header.count, std::memory_order_relaxed);

        // Everything below is per batch, per client or per datagram, not per event.
        if (m_config.egressConsoleEcho) {
            std::cout << m_echo.str();
            m_echo.str(std::string());
            m_echo.clear();
        }
        for (size_t i = 0; i < m_sessionsUsed; ++i) {
            // Private ack — silently dropped if the originating client has
            // since disconnected (a stale/unknown sessionId is expected,
            // not an error; see NetworkGateway::sendToSession).
            m_gateway.sendToSession(m_sessions[i].id, m_sessions[i].bytes.data(), m_sessions[i].bytes.size());
        }
        m_sessionsUsed = 0;
        flushFeed();
    }

    // Replicas are deterministic, so an ordinal names the same event on every replica. The next
    // ordinal expected is therefore shared. A jump forward means events nobody published, for
    // example the ones a new leader consumed as a follower. An overlap means events already seen.
    void accountForOrdinals(const EgressBatchHeader& header) {
        const uint64_t end = header.firstOrdinal + header.count;
        if (header.firstOrdinal > m_nextOrdinal) {
            m_missingEvents.fetch_add(header.firstOrdinal - m_nextOrdinal, std::memory_order_relaxed);
        }
        else if (header.firstOrdinal < m_nextOrdinal) {
            m_repeatedEvents.fetch_add(std::min<uint64_t>(header.count, m_nextOrdinal - header.firstOrdinal),
                std::memory_order_relaxed);
        }
        m_nextOrdinal = std::max(m_nextOrdinal, end);
    }

    // This batch's output for one client. A batch holds few clients, so a scan is cheapest.
    SessionOutput& sessionOutput(SessionId id) {
        for (size_t i = 0; i < m_sessionsUsed; ++i) {
            if (m_sessions[i].id == id) return m_sessions[i];
        }
        if (m_sessionsUsed == m_sessions.size()) m_sessions.emplace_back();
        SessionOutput& slot = m_sessions[m_sessionsUsed++];
        slot.id = id;
        slot.bytes.clear();
        return slot;
    }

    // Frames go into the current public datagram until the next one would not fit.
    void appendToFeed(const char* data, size_t length) {
        if (!m_feed.empty() && m_feed.size() + length > m_config.egressDatagramMaxBytes) flushFeed();
        m_feed.insert(m_feed.end(), data, data + length);
    }
    void flushFeed() {
        if (m_feed.empty()) return;
        sendto(m_broadcastFd, m_feed.data(), m_feed.size(), 0,
            (struct sockaddr*)&m_broadcastDestAddr, sizeof(m_broadcastDestAddr));
        m_feed.clear();
    }

    // A gateway that keeps output for a client whose socket is full offers this. Others need not.
    void flushPendingSessions() {
        if constexpr (requires(TGateway& g) { g.flushPendingSessions(); }) {
            m_gateway.flushPendingSessions();
        }
    }

    TGateway& m_gateway;
    NetworkConfig m_config;
    const ReplicaArbiter& m_arbiter;
    std::atomic<bool> m_running{ false };
    std::thread m_worker;
    int m_rxFd{ -1 };
    int m_broadcastFd{ -1 };
    int m_receiveBufferBytes{ 0 };
    sockaddr_in m_broadcastDestAddr{};

    // Owned by the receive thread.
    uint64_t m_nextOrdinal = 0;
    uint64_t m_reportedMissing = 0;
    bool m_reportedAny = false;
    std::chrono::steady_clock::time_point m_lastLossReport;
    std::vector<SessionOutput> m_sessions;
    size_t m_sessionsUsed = 0;
    std::vector<char> m_feed;
    std::ostringstream m_echo;

    std::atomic<uint64_t> m_batches{ 0 };
    std::atomic<uint64_t> m_events{ 0 };
    std::atomic<uint64_t> m_staleBatches{ 0 };
    std::atomic<uint64_t> m_staleEvents{ 0 };
    std::atomic<uint64_t> m_malformed{ 0 };
    std::atomic<uint64_t> m_missingEvents{ 0 };
    std::atomic<uint64_t> m_repeatedEvents{ 0 };
};
