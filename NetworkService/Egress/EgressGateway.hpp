#pragma once
#include "FencedMessage.hpp"
#include "NetworkConfig.hpp"
#include "OrderEventFrame.hpp"
#include "ReplicaArbiter.hpp"

#include <atomic>
#include <cstring>
#include <iostream>
#include <thread>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

// Runs inside NetworkServiceApp: joins the internal egress multicast channel
// (see NetworkConfig::egressMulticastIp/Port) that every MatchingServiceApp
// replica's EgressPublisher writes to, and for each FencedMessage<TEvent>
// received:
//   - drops it if its epoch is stale (see ReplicaArbiter — this is what
//     actually makes a failed-over-away-from replica's late traffic
//     harmless, rather than requiring it to notice its own demotion)
//   - prints it to console (stand-in for a real market-data/ack consumer
//     while there's no client that can fully decode the wire frame yet)
//   - sends a private OUCH-style ack back over the originating client's own
//     TCP connection, via TGateway::sendToSession (a no-op if that client
//     has since disconnected)
//   - broadcasts the same encoded bytes to the public ITCH multicast group
//
// TGateway is NetworkGateway<TProtocolHandler, TCommand> — templated here
// rather than named directly so this header doesn't need to know either of
// NetworkGateway's own template parameters.
template <typename TGateway, typename TEvent>
class EgressGateway {
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

        m_itchFd = socket(AF_INET, SOCK_DGRAM, 0);
        unsigned char ttl = 1;
        setsockopt(m_itchFd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
        std::memset(&m_itchDestAddr, 0, sizeof(m_itchDestAddr));
        m_itchDestAddr.sin_family = AF_INET;
        m_itchDestAddr.sin_port = htons(m_config.itchMulticastPort);
        inet_pton(AF_INET, m_config.itchMulticastGroup.c_str(), &m_itchDestAddr.sin_addr);

        m_running.store(true);
        m_worker = std::thread(&EgressGateway::rxLoop, this);
    }

    void stop() {
        if (!m_running.load()) return;
        m_running.store(false);
        if (m_worker.joinable()) m_worker.join();
        if (m_rxFd != -1) close(m_rxFd);
        if (m_itchFd != -1) close(m_itchFd);
        m_rxFd = -1;
        m_itchFd = -1;
    }

private:
    void rxLoop() {
        while (m_running.load(std::memory_order_relaxed)) {
            FencedMessage<TEvent> msg{};
            ssize_t len = recv(m_rxFd, &msg, sizeof(msg), 0);
            if (len != static_cast<ssize_t>(sizeof(msg))) continue;

            // Zero-latency, in-process check — no network round trip needed
            // since the arbiter that decided this epoch lives right here.
            if (msg.epoch < m_arbiter.currentEpoch()) continue; // stale, from a deposed leader

            const TEvent& evt = msg.payload;
            printEvent(evt);

            auto frame = EncodeOrderEventFrame(evt);

            // Private ack — silently dropped if the originating client has
            // since disconnected (a stale/unknown sessionId is expected,
            // not an error; see NetworkGateway::sendToSession).
            m_gateway.sendToSession(evt.session_id, frame.data(), frame.size());

            // Public feed — always broadcast regardless of session.
            sendto(m_itchFd, frame.data(), frame.size(), 0,
                (struct sockaddr*)&m_itchDestAddr, sizeof(m_itchDestAddr));
        }
    }

    static void printEvent(const TEvent& evt) {
        std::cout << "[egress] type=" << EncodeOrderEventTypeByte(evt.type)
                  << " order_id=" << evt.order_id
                  << " session_id=" << evt.session_id
                  << " side=" << (evt.side == SIDE::BID ? 'B' : evt.side == SIDE::ASK ? 'S' : '-')
                  << " price=" << static_cast<int>(evt.price)
                  << " qty=" << evt.quantity
                  << " leaves=" << evt.leaves_quantity
                  << " match_id=" << evt.match_id
                  << " reject_reason=" << static_cast<int>(evt.reject_reason)
                  << "\n";
    }

    TGateway& m_gateway;
    NetworkConfig m_config;
    const ReplicaArbiter& m_arbiter;
    std::atomic<bool> m_running{ false };
    std::thread m_worker;
    int m_rxFd{ -1 };
    int m_itchFd{ -1 };
    sockaddr_in m_itchDestAddr{};
};
