#pragma once
#include <atomic>
#include <cerrno>
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
#include "MulticastFanOut.hpp"
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
//
// Fan-out batching: the commands sequenced in one pass of the ingress loop
// leave as one multicast datagram of back-to-back records (at most
// NetworkConfig::ingressDatagramMaxBytes), sent at the end of that pass or as
// soon as the datagram is full. Nothing ever waits for more commands to
// arrive, so a command that arrives alone goes out at once, in a datagram of
// one record. Every record is appended to the store before its datagram is sent.
//
// TFanOut is how a batch reaches the replicas (see MulticastFanOut.hpp for the
// required shape). The default is one UDP multicast datagram per batch.
template <typename TProtocolHandler, typename TCommand, typename TFanOut = MulticastFanOut>
class NetworkGateway {
public:
    NetworkGateway(TProtocolHandler& handler, const NetworkConfig& config,
                   const std::string& sequenceStorePath, size_t sequenceStoreCapacity)
        : m_protocolHandler(handler), m_config(config),
          m_sequenceStore(sequenceStorePath, sequenceStoreCapacity), m_fanOut(config) {
    }

    ~NetworkGateway() { stop(); }

    void start();
    void stop();

    // Exposes the durable sequence log so a RetransmitServer<TCommand> can be
    // wired to it from outside (gap recovery is a separate process concern,
    // not something NetworkGateway serves itself).
    SequenceStore<TCommand>& sequenceStore() { return m_sequenceStore; }

    // The fan-out, for a caller that needs to ask the transport something its
    // own type knows (for example, whether any replica is connected yet).
    const TFanOut& fanOut() const { return m_fanOut; }

    // Best-effort private send to whichever live connection owns `id`, called
    // from EgressGateway's thread (not the ingress thread) — the only part of
    // NetworkGateway that isn't single-threaded-by-design. Returns false with
    // no error if that session has since disconnected (a stale/aged-out
    // sessionId is a safe no-op here, unlike a raw fd lookup would be).
    //
    // `data` should be whole frames. A client's socket may take only part of
    // a call, or none of it. The rest is kept, and goes out before anything
    // newer, so the client's stream stays in order and no frame is ever cut by
    // the one after it. If the client falls so far behind that more than
    // NetworkConfig::sessionOutboundBufferBytes is waiting, a new call is
    // dropped whole and counted (droppedOutputBytes), never cut mid-frame.
    bool sendToSession(SessionId id, const char* data, size_t len) {
        std::lock_guard<std::mutex> lock(m_sessionMutex);
        auto it = m_sessionOutput.find(id);
        if (it == m_sessionOutput.end()) return false;
        SessionOutput& session = it->second;

        if (!session.pending.empty()) {
            if (!drainPending(session)) return false;
            if (!session.pending.empty()) {
                // Still backed up: queue behind the older output, within the limit.
                if (session.pending.size() + len > m_config.sessionOutboundBufferBytes) {
                    m_droppedOutputBytes.fetch_add(len, std::memory_order_relaxed);
                    return false;
                }
                session.pending.append(data, len);
                return true;
            }
            trackPending(session);
        }

        // Nothing waiting: write straight away. What the socket does not take must be kept,
        // whatever the limit, because dropping the rest of a frame already begun would corrupt the stream.
        size_t sent = 0;
        while (sent < len) {
            const ssize_t n = send(session.fd, data + sent, len - sent, MSG_NOSIGNAL);
            if (n > 0) { sent += static_cast<size_t>(n); continue; }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            return false; // the peer is gone; the ingress thread closes the connection when it notices
        }
        if (sent < len) {
            session.pending.assign(data + sent, len - sent);
            trackPending(session);
        }
        return true;
    }

    // Gives every client with output waiting another chance to take it. EgressGateway calls this after each
    // batch and while it is idle, so a slow client catches up even when no new output arrives for it.
    void flushPendingSessions() {
        if (m_sessionsWithPending.load(std::memory_order_relaxed) == 0) return;
        std::lock_guard<std::mutex> lock(m_sessionMutex);
        for (auto& entry : m_sessionOutput) {
            SessionOutput& session = entry.second;
            if (session.pending.empty()) continue;
            drainPending(session);
            trackPending(session);
        }
    }

    // Output bytes that a client's socket has not yet taken, over all clients.
    size_t pendingOutputBytes() const {
        std::lock_guard<std::mutex> lock(m_sessionMutex);
        size_t total = 0;
        for (const auto& entry : m_sessionOutput) total += entry.second.pending.size();
        return total;
    }

    // Bytes of output dropped because a client was too far behind. See sendToSession.
    uint64_t droppedOutputBytes() const { return m_droppedOutputBytes.load(std::memory_order_relaxed); }

private:
    void pollSockets();
    // Multicasts the records collected so far as one datagram and empties the
    // batch. Every record in it is already in the sequence store.
    void flushBatch();
    // Full teardown for one connection: stop polling its fd, close it, and
    // erase every piece of per-connection state. The single canonical place
    // this happens — called both when recv() reports the peer is gone and
    // when validateAndParse reports a structurally-broken frame.
    void closeConnection(int fd);

    // One client's private output: its socket, and the bytes the socket has not yet taken.
    struct SessionOutput {
        int fd = -1;
        std::string pending;     // oldest first
        bool counted = false;    // included in m_sessionsWithPending
    };

    // Writes as much of the client's waiting output as its socket takes. Returns false if the peer is gone.
    // m_sessionMutex must be held.
    bool drainPending(SessionOutput& session) {
        while (!session.pending.empty()) {
            const ssize_t n = send(session.fd, session.pending.data(), session.pending.size(), MSG_NOSIGNAL);
            if (n > 0) { session.pending.erase(0, static_cast<size_t>(n)); continue; }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
            return false;
        }
        return true;
    }

    // Keeps m_sessionsWithPending right, so flushPendingSessions() costs nothing when no client is behind.
    // m_sessionMutex must be held.
    void trackPending(SessionOutput& session) {
        const bool behind = !session.pending.empty();
        if (behind == session.counted) return;
        session.counted = behind;
        if (behind) m_sessionsWithPending.fetch_add(1, std::memory_order_relaxed);
        else m_sessionsWithPending.fetch_sub(1, std::memory_order_relaxed);
    }

    TProtocolHandler& m_protocolHandler;
    NetworkConfig       m_config;
    SequenceStore<TCommand>     m_sequenceStore;

    std::atomic<bool>   m_running{ false };
    std::thread         m_ingressThread;

    int m_listenSocket = -1;
    int m_epollFd = -1;
    TFanOut m_fanOut;

    static constexpr size_t BUFFER_SIZE = 4096;
    char m_readBuffer[BUFFER_SIZE];

    uint64_t m_sequenceNumber = 0; // single-writer (ingress thread only)

    // Records sequenced in the current epoll round, waiting to go out as one
    // datagram. Ingress thread only. Capacity is fixed in start().
    std::vector<SequencedInboundMessage<TCommand>> m_batch;
    size_t m_recordsPerDatagram = 1;
    std::unordered_map<int, ConnectionBuffer> m_connBuffers;

    // Session bookkeeping for egress routing (see sendToSession above).
    // m_nextSessionId/m_fdToSession are ingress-thread-only, same as
    // m_connBuffers. m_sessionOutput is the only piece used from another
    // thread (EgressGateway's), hence the separate mutex — kept off the
    // lock-free ingress hot path entirely. The ingress thread takes it only
    // when a client connects or leaves.
    SessionId m_nextSessionId{ 1 };
    std::unordered_map<int, SessionId> m_fdToSession;
    mutable std::mutex m_sessionMutex;
    std::unordered_map<SessionId, SessionOutput> m_sessionOutput;
    std::atomic<size_t> m_sessionsWithPending{ 0 };
    std::atomic<uint64_t> m_droppedOutputBytes{ 0 };
};

#include "NetworkGateway.inl" // template implementation, see below