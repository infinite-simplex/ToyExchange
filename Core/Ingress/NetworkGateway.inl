#pragma once
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>

namespace detail {
    inline void makeNonBlocking(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags < 0) {
            close(fd);
            throw std::system_error(errno, std::generic_category(), "Unable to get file descriptor flags");
        }
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
            close(fd);
            throw std::system_error(errno, std::generic_category(), "Unable to set file descriptor as non-blocking");
        }
    }
}

template <typename TProtocolHandler, typename TCommand, typename TFanOut>
void NetworkGateway<TProtocolHandler, TCommand, TFanOut>::start() {
    if (m_running.load()) return;

    m_listenSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (m_listenSocket < 0) {
        throw std::system_error(errno, std::generic_category(), "Unable to create TCP listen socket");
    }
    detail::makeNonBlocking(m_listenSocket);
    int opt = 1;
    if (setsockopt(m_listenSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        close(m_listenSocket);
        throw std::system_error(errno, std::generic_category(), "Unable to set TCP listen socket options");
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(m_config.ingressListenPort);
    if (bind(m_listenSocket, (struct sockaddr*)&address, sizeof(address)) < 0) {
        close(m_listenSocket);
        throw std::system_error(errno, std::generic_category(), "Unable to bind TCP listen socket");
    }
    if (listen(m_listenSocket, SOMAXCONN) < 0) {
        close(m_listenSocket);
        throw std::system_error(errno, std::generic_category(), "Unable to listen on TCP socket");
    }

    m_epollFd = epoll_create1(0);
    if (m_epollFd < 0) {
        close(m_listenSocket);
        throw std::system_error(errno, std::generic_category(), "Unable to create epoll instance");
    }
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = m_listenSocket;
    if (epoll_ctl(m_epollFd, EPOLL_CTL_ADD, m_listenSocket, &ev) < 0) {
        close(m_epollFd);
        close(m_listenSocket);
        throw std::system_error(errno, std::generic_category(), "Unable to register listen socket with epoll");
    }

    try {
        m_fanOut.start();
    }
    catch (...) {
        close(m_epollFd);
        close(m_listenSocket);
        throw;
    }

    m_recordsPerDatagram = RecordsPerDatagram<TCommand>(m_config.ingressDatagramMaxBytes);
    m_batch.clear();
    m_batch.reserve(m_recordsPerDatagram);

    m_running.store(true);
    m_ingressThread = std::thread(&NetworkGateway::pollSockets, this);
}

template <typename TProtocolHandler, typename TCommand, typename TFanOut>
void NetworkGateway<TProtocolHandler, TCommand, TFanOut>::stop() {
    if (!m_running.load()) return;
    m_running.store(false);

    if (m_ingressThread.joinable()) m_ingressThread.join();

    if (m_epollFd != -1) close(m_epollFd);
    if (m_listenSocket != -1) close(m_listenSocket);
    m_fanOut.stop();

    m_epollFd = -1;
    m_listenSocket = -1;
    m_connBuffers.clear();
}

template <typename TProtocolHandler, typename TCommand, typename TFanOut>
void NetworkGateway<TProtocolHandler, TCommand, TFanOut>::pollSockets() {
    constexpr int MAX_EVENTS = 64;
    epoll_event events[MAX_EVENTS];

    while (m_running.load(std::memory_order_relaxed)) {
        int numEvents = epoll_wait(m_epollFd, events, MAX_EVENTS, 1);
        if (numEvents < 0) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < numEvents; ++i) {
            int currentFd = events[i].data.fd;

            if (currentFd == m_listenSocket) {
                while (true) {
                    int clientSocket = accept(m_listenSocket, nullptr, nullptr);
                    if (clientSocket < 0) break;
                    detail::makeNonBlocking(clientSocket);
                    epoll_event childEv{};
                    childEv.events = EPOLLIN | EPOLLRDHUP;
                    childEv.data.fd = clientSocket;
                    epoll_ctl(m_epollFd, EPOLL_CTL_ADD, clientSocket, &childEv);

                    SessionId sessionId = m_nextSessionId++;
                    m_fdToSession[clientSocket] = sessionId;
                    {
                        std::lock_guard<std::mutex> lock(m_sessionMutex);
                        m_sessionOutput[sessionId].fd = clientSocket;
                    }
                }
                continue;
            }

            uint64_t ingressTaiNs = get_synced_time_ns(); // cross-machine-comparable — see Telemetry.hpp
            ssize_t bytesRead = recv(currentFd, m_readBuffer, BUFFER_SIZE, MSG_DONTWAIT);

            if (bytesRead <= 0) {
                if (bytesRead < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) continue;
                closeConnection(currentFd);
                continue;
            }

            ConnectionBuffer& connBuf = m_connBuffers[currentFd];
            connBuf.append(m_readBuffer, static_cast<size_t>(bytesRead));

            while (true) {
                TCommand cmd{};
                size_t consumed = m_protocolHandler.validateAndParse(
                    connBuf.m_data.data(), connBuf.m_validBytes, cmd);
                if (consumed == TProtocolHandler::PARSE_FATAL_ERROR) {
                    closeConnection(currentFd); // connBuf now dangles (erased below) — don't touch it after this
                    break;
                }
                if (consumed == 0) break;

                const uint64_t seq = m_sequenceNumber++;

                // The sequence number is already a single-writer, monotonic,
                // per-command identifier — reuse it as the telemetry trace_id
                // instead of maintaining a second counter. Arena lookups always
                // go through trace_index(), so the eventual truncation/wrap into
                // TraceId (see alias.hpp) and the eventual wrap of the arena
                // ring itself are both safe.
                cmd.trace_id = static_cast<decltype(cmd.trace_id)>(seq);
                cmd.sessionId = m_fdToSession[currentFd];

                SequencedInboundMessage<TCommand> msg{};
                msg.sequenceNumber = seq;
                msg.ingressTaiNs = ingressTaiNs;
                msg.clientSessionId = cmd.sessionId;
                msg.command = cmd;
                m_sequenceStore.append(msg); // append before multicast — closes the durability gap
                m_batch.push_back(msg);
                if (m_batch.size() == m_recordsPerDatagram) flushBatch();

                connBuf.consume(consumed);
            }
        }

        // One datagram for everything this round sequenced, however many
        // connections it came from. Nothing waits for a datagram to fill: at a
        // low rate this is one record per datagram, and the batch grows only
        // as fast as commands arrive.
        flushBatch();
    }
}

template <typename TProtocolHandler, typename TCommand, typename TFanOut>
void NetworkGateway<TProtocolHandler, TCommand, TFanOut>::flushBatch() {
    if (m_batch.empty()) return;
    // A failed send loses no command: every record is already in the store,
    // so each replica sees the gap and fetches the records through the
    // retransmit server.
    m_fanOut.publish(m_batch.data(), m_batch.size() * sizeof(m_batch[0]));
    m_batch.clear();
}

template <typename TProtocolHandler, typename TCommand, typename TFanOut>
void NetworkGateway<TProtocolHandler, TCommand, TFanOut>::closeConnection(int fd) {
    epoll_ctl(m_epollFd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    m_connBuffers.erase(fd);
    auto sessionIt = m_fdToSession.find(fd);
    if (sessionIt != m_fdToSession.end()) {
        std::lock_guard<std::mutex> lock(m_sessionMutex);
        auto output = m_sessionOutput.find(sessionIt->second);
        if (output != m_sessionOutput.end()) {
            if (output->second.counted) m_sessionsWithPending.fetch_sub(1, std::memory_order_relaxed);
            m_sessionOutput.erase(output);
        }
        m_fdToSession.erase(sessionIt);
    }
}