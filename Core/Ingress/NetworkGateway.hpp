#pragma once
#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <string>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include "Alias.hpp" // SessionId
#include "SequenceStore.hpp"
#include "SequencedInboundMessage.hpp"
#include "NetworkConfig.hpp"
#include "Telemetry.hpp" // get_time(), used in NetworkGateway.inl

struct ConnectionBuffer {
    std::vector<char> m_data;
    size_t m_validBytes = 0;

    void append(const char* src, size_t len) {
        if (m_validBytes + len > m_data.size()) m_data.resize(m_validBytes + len);
        std::memcpy(m_data.data() + m_validBytes, src, len);
        m_validBytes += len;
    }
    void consume(size_t len) {
        std::memmove(m_data.data(), m_data.data() + len, m_validBytes - len);
        m_validBytes -= len;
    }
};

// TProtocolHandler must provide:
//   size_t validateAndParse(const char* buf, size_t availableLen, TCommand& outCmd);
// returning bytes consumed (>0), 0 if a full frame isn't available yet, or
// TProtocolHandler::PARSE_FATAL_ERROR if a complete frame was received but
// is structurally uninterpretable — NetworkGateway closes the connection in
// that case rather than stalling on bytes that will never parse.
template <typename TProtocolHandler, typename TCommand>
class NetworkGateway {
public:
    NetworkGateway(TProtocolHandler& handler, const NetworkConfig& config,
                   const std::string& sequenceStorePath, size_t sequenceStoreCapacity)
        : m_protocolHandler(handler), m_config(config),
          m_sequenceStore(sequenceStorePath, sequenceStoreCapacity) {
    }

    ~NetworkGateway() { stop(); }

    void start();
    void stop();

    // Exposes the durable sequence log so a RetransmitServer<TCommand> can be
    // wired to it from outside (gap recovery is a separate process concern,
    // not something NetworkGateway serves itself).
    SequenceStore<TCommand>& sequenceStore() { return m_sequenceStore; }

    // Best-effort private send to whichever live connection owns `id`, called
    // from EgressGateway's thread (not the ingress thread) — the only part of
    // NetworkGateway that isn't single-threaded-by-design. Returns false with
    // no error if that session has since disconnected (a stale/aged-out
    // sessionId is a safe no-op here, unlike a raw fd lookup would be).
    bool sendToSession(SessionId id, const char* data, size_t len) {
        int fd = -1;
        {
            std::lock_guard<std::mutex> lock(m_sessionMutex);
            auto it = m_sessionToFd.find(id);
            if (it == m_sessionToFd.end()) return false;
            fd = it->second;
        }
        return send(fd, data, len, MSG_NOSIGNAL) >= 0;
    }

private:
    void pollSockets();
    // Full teardown for one connection: stop polling its fd, close it, and
    // erase every piece of per-connection state. The single canonical place
    // this happens — called both when recv() reports the peer is gone and
    // when validateAndParse reports a structurally-broken frame.
    void closeConnection(int fd);

    TProtocolHandler& m_protocolHandler;
    NetworkConfig       m_config;
    SequenceStore<TCommand>     m_sequenceStore;

    std::atomic<bool>   m_running{ false };
    std::thread         m_ingressThread;

    int m_listenSocket = -1;
    int m_epollFd = -1;
    int m_mcastFd = -1;
    sockaddr_in m_mcastDestAddr{};

    static constexpr size_t BUFFER_SIZE = 4096;
    char m_readBuffer[BUFFER_SIZE];

    uint64_t m_sequenceNumber = 0; // single-writer (ingress thread only)
    std::unordered_map<int, ConnectionBuffer> m_connBuffers;

    // Session bookkeeping for egress routing (see sendToSession above).
    // m_nextSessionId/m_fdToSession are ingress-thread-only, same as
    // m_connBuffers. m_sessionToFd is the only piece read from another
    // thread (EgressGateway's), hence the separate mutex — kept off the
    // lock-free ingress hot path entirely.
    SessionId m_nextSessionId{ 1 };
    std::unordered_map<int, SessionId> m_fdToSession;
    std::mutex m_sessionMutex;
    std::unordered_map<SessionId, int> m_sessionToFd;
};

#include "NetworkGateway.inl" // template implementation, see below