#pragma once
#include <thread>
#include <atomic>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <system_error>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include "NetworkConfig.hpp"
#include "SequenceStore.hpp"
#include "RetransmitMessage.hpp"

// Answers gap-fill requests from MulticastIngressReceiver. A request names a
// start sequence and a count; the reply is a few datagrams, each holding a
// RetransmitReplyHeader and as many whole records as fit in maxDatagramBytes
// (see RetransmitMessage.hpp). The datagram size limit is the same
// NetworkConfig::ingressDatagramMaxBytes the gateway uses for its fan-out,
// so pass the same value when it is not the default.
template <typename TCommand>
class RetransmitServer {
public:
    RetransmitServer(SequenceStore<TCommand>& store, uint16_t listenPort,
        size_t maxDatagramBytes = DEFAULT_DATAGRAM_MAX_BYTES)
        : m_store(store), m_listenPort(listenPort),
          m_recordsPerDatagram(RecordsPerDatagram<TCommand>(maxDatagramBytes, sizeof(RetransmitReplyHeader))),
          m_packet(sizeof(RetransmitReplyHeader) + m_recordsPerDatagram * sizeof(SequencedInboundMessage<TCommand>)) {
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
    using Message = SequencedInboundMessage<TCommand>;

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
            reply(req.startSeq, std::min(req.count, MAX_BATCH), fromAddr, fromLen);
        }
    }

    // Sends records startSeq .. startSeq+count-1, a datagram at a time. When a
    // record is not available it sends the records before it, then one status
    // datagram naming that record, and stops: the rest of the range is equally
    // unavailable either way.
    void reply(uint64_t startSeq, uint32_t count, const sockaddr_in& to, socklen_t toLen) {
        uint32_t next = 0;
        while (next < count) {
            uint32_t inPacket = 0;
            auto stopped = SequenceStore<TCommand>::ReadResult::OK;

            while (inPacket < m_recordsPerDatagram && next < count) {
                Message record;
                stopped = m_store.read(startSeq + next, record);
                if (stopped != SequenceStore<TCommand>::ReadResult::OK) break;
                std::memcpy(m_packet.data() + sizeof(RetransmitReplyHeader) + inPacket * sizeof(Message),
                    &record, sizeof(record));
                ++inPacket;
                ++next;
            }

            if (inPacket > 0) {
                RetransmitReplyHeader hdr{ RetransmitStatus::OK, {}, inPacket, startSeq + next - inPacket };
                std::memcpy(m_packet.data(), &hdr, sizeof(hdr));
                sendto(m_socket, m_packet.data(), sizeof(hdr) + inPacket * sizeof(Message), 0,
                    (const sockaddr*)&to, toLen);
            }
            if (stopped != SequenceStore<TCommand>::ReadResult::OK) {
                const auto status = (stopped == SequenceStore<TCommand>::ReadResult::TOO_OLD)
                    ? RetransmitStatus::TOO_OLD : RetransmitStatus::NOT_YET_ARRIVED;
                RetransmitReplyHeader hdr{ status, {}, 0, startSeq + next };
                sendto(m_socket, &hdr, sizeof(hdr), 0, (const sockaddr*)&to, toLen);
                return;
            }
        }
    }

    SequenceStore<TCommand>& m_store;
    uint16_t m_listenPort;
    size_t m_recordsPerDatagram;
    std::vector<char> m_packet; // one reply datagram, reused; serve thread only
    int m_socket = -1;
    std::atomic<bool> m_running{ false };
    std::thread m_thread;
};
