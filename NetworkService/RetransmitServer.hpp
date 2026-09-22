#pragma once
#include <thread>
#include <atomic>
#include <algorithm>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include "SequenceStore.hpp"
#include "RetransmitMessage.hpp"

template <typename TCommand>
class RetransmitServer {
public:
    RetransmitServer(SequenceStore<TCommand>& store, uint16_t listenPort)
        : m_store(store), m_listenPort(listenPort) {
    }

    ~RetransmitServer() { stop(); }

    void start() {
        m_socket = socket(AF_INET, SOCK_DGRAM, 0);
        if (m_socket < 0) {
            throw std::system_error(errno, std::generic_category(), "Unable to create retransmit socket");
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(m_listenPort);
        if (bind(m_socket, (sockaddr*)&addr, sizeof(addr)) < 0) {
            close(m_socket);
            throw std::system_error(errno, std::generic_category(), "Unable to bind retransmit socket");
        }
        m_running.store(true);
        m_thread = std::thread(&RetransmitServer::serveLoop, this);
    }

    void stop() {
        if (!m_running.load()) return;
        m_running.store(false);
        if (m_socket != -1) { shutdown(m_socket, SHUT_RDWR); close(m_socket); }
        if (m_thread.joinable()) m_thread.join();
        m_socket = -1;
    }

private:
    static constexpr uint32_t MAX_BATCH = 500; // caps worst-case work per request

    void serveLoop() {
        char reqBuf[sizeof(RetransmitRequest)];

        while (m_running.load(std::memory_order_relaxed)) {
            sockaddr_in fromAddr{};
            socklen_t fromLen = sizeof(fromAddr);

            ssize_t n = recvfrom(m_socket, reqBuf, sizeof(reqBuf), 0,
                (sockaddr*)&fromAddr, &fromLen);
            if (n != sizeof(RetransmitRequest)) continue; // malformed, ignore

            RetransmitRequest req;
            std::memcpy(&req, reqBuf, sizeof(req));
            uint32_t count = std::min(req.count, MAX_BATCH);

            for (uint32_t i = 0; i < count; ++i) {
                uint64_t seq = req.startSeq + i;
                SequencedInboundMessage<TCommand> record;
                auto result = m_store.read(seq, record);

                if (result == SequenceStore<TCommand>::ReadResult::OK) {
                    RetransmitRecordHeader hdr{ RetransmitStatus::OK, {}, seq };
                    char packet[sizeof(hdr) + sizeof(record)];
                    std::memcpy(packet, &hdr, sizeof(hdr));
                    std::memcpy(packet + sizeof(hdr), &record, sizeof(record));
                    sendto(m_socket, packet, sizeof(packet), 0, (sockaddr*)&fromAddr, fromLen);
                }
                else {
                    auto status = (result == SequenceStore<TCommand>::ReadResult::TOO_OLD)
                        ? RetransmitStatus::TOO_OLD : RetransmitStatus::NOT_YET_ARRIVED;
                    RetransmitRecordHeader hdr{ status, {}, seq };
                    sendto(m_socket, &hdr, sizeof(hdr), 0, (sockaddr*)&fromAddr, fromLen);
                    break; // rest of the batch is equally unavailable either way
                }
            }
        }
    }

    SequenceStore<TCommand>& m_store;
    uint16_t m_listenPort;
    int m_socket = -1;
    std::atomic<bool> m_running{ false };
    std::thread m_thread;
};