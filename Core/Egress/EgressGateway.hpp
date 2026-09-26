#pragma once
#include "FencedMessage.hpp"
#include "NetworkConfig.hpp"
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
// every MatchingServiceApp replica's EgressPublisher writes to, and for each
// FencedMessage<TEvent> received: drops it if its epoch is stale (see
// ReplicaArbiter — this is what makes a failed-over replica's late traffic
// harmless without it needing to notice its own demotion), prints it to
// console (stand-in for a real market-data consumer), sends a private ack
// back over the originating client's own TCP connection via
// TGateway::sendToSession (a no-op if that client has since disconnected),
// and broadcasts the same encoded bytes to the public broadcast group.
//
// TGateway is NetworkGateway<TProtocolHandler, TCommand> — templated here so
// this header doesn't need to know either of its own template parameters.
// TEncoder converts a TEvent into wire bytes and formats it for the console
// (static encode()/print() — see e.g. OuchEventEncoder in OrderEventFrame.hpp)
// — this header has no knowledge of any specific product's event format.
template <typename TGateway, typename TEvent, typename TEncoder>
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

            auto frame = TEncoder::encode(evt);

            // Private ack — silently dropped if the originating client has
            // since disconnected (a stale/unknown sessionId is expected,
            // not an error; see NetworkGateway::sendToSession).
            m_gateway.sendToSession(evt.session_id, frame.data(), frame.size());

            // Public feed — always broadcast regardless of session.
            sendto(m_broadcastFd, frame.data(), frame.size(), 0,
                (struct sockaddr*)&m_broadcastDestAddr, sizeof(m_broadcastDestAddr));
        }
    }

    static void printEvent(const TEvent& evt) {
        std::cout << "[egress] ";
        TEncoder::print(std::cout, evt);
        std::cout << "\n";
    }

    TGateway& m_gateway;
    NetworkConfig m_config;
    const ReplicaArbiter& m_arbiter;
    std::atomic<bool> m_running{ false };
    std::thread m_worker;
    int m_rxFd{ -1 };
    int m_broadcastFd{ -1 };
    sockaddr_in m_broadcastDestAddr{};
};
