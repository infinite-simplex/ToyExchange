#pragma once
#include <IGoodTillDayScheduler.hpp>
#include "SPSCQueue.hpp"
#include "NetworkConfig.hpp"

#include <atomic>
#include <thread>
#include <chrono>
#include <queue>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

struct ScheduledCancel {
    char orderToken[14]{};
    std::chrono::system_clock::time_point cancelAt;
};

// The GTD auto-cancel thread: architecturally just another OUCH client. Fires
// GOOD_TILL_DAY cancellations registered via schedule_cancel() (called from
// the matching thread — see IGoodTillDayScheduler) by sending a real OUCH
// CANCEL_ORDER frame over a persistent TCP connection to NetworkGateway's own
// OUCH listen port, exactly as a real client would — reusing the entire
// existing pipeline (durability, sequencing, trace_id, telemetry) for free.
class GtdCancelService : public IGoodTillDayScheduler {
public:
    explicit GtdCancelService(const NetworkConfig& config) : m_config(config) {}
    ~GtdCancelService() { stop(); }

    void start() {
        if (m_running.load()) return;
        m_running.store(true);
        m_thread = std::thread(&GtdCancelService::run, this);
    }

    void stop() {
        if (!m_running.load()) return;
        m_running.store(false);
        if (m_thread.joinable()) m_thread.join();
        if (m_socket != -1) { close(m_socket); m_socket = -1; }
    }

    // Best-effort: never blocks the caller (the matching thread). A dropped
    // schedule just means that order isn't auto-canceled at 4pm — degraded,
    // not catastrophic, same principle as on_trace_complete's drop-on-full.
    void schedule_cancel(const char orderToken[14], std::chrono::system_clock::time_point cancelAt) override {
        ScheduledCancel req{};
        std::memcpy(req.orderToken, orderToken, 14);
        req.cancelAt = cancelAt;
        m_scheduleQueue.try_push(req);
    }

private:
    struct ByCancelTimeAscending {
        bool operator()(const ScheduledCancel& a, const ScheduledCancel& b) const noexcept {
            return a.cancelAt > b.cancelAt; // min-heap: earliest cancelAt on top
        }
    };

    void run() {
        while (m_running.load(std::memory_order_relaxed)) {
            ScheduledCancel req;
            while (m_scheduleQueue.try_pop(req)) {
                m_heap.push(req);
            }

            auto now = std::chrono::system_clock::now();
            while (!m_heap.empty() && m_heap.top().cancelAt <= now) {
                send_cancel(m_heap.top().orderToken);
                m_heap.pop();
            }

            // Flat poll interval directly bounds fire latency to ~10ms of the
            // scheduled time, plus whatever this iteration's send work costs.
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    bool ensure_connected() {
        if (m_socket != -1) return true;

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return false;

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(m_config.ouchListenPort);
        inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr); // same-host as NetworkGateway

        if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
            close(sock);
            return false; // NetworkGateway may not be up yet — retried on the next fire
        }

        m_socket = sock;
        return true;
    }

    void send_cancel(const char orderToken[14]) {
        if (!ensure_connected()) return;

        constexpr size_t bodyLen = 14 + 4; // orderToken + shares(=0, full cancel)
        constexpr uint16_t payloadLen = static_cast<uint16_t>(1 + bodyLen);
        char frame[2 + payloadLen];

        size_t off = 0;
        frame[off++] = static_cast<char>((payloadLen >> 8) & 0xFF);
        frame[off++] = static_cast<char>(payloadLen & 0xFF);
        frame[off++] = 'X';
        std::memcpy(frame + off, orderToken, 14);
        off += 14;
        frame[off++] = 0; frame[off++] = 0; frame[off++] = 0; frame[off++] = 0;

        ssize_t sent = send(m_socket, frame, sizeof(frame), 0);
        if (sent != static_cast<ssize_t>(sizeof(frame))) {
            close(m_socket);
            m_socket = -1; // reconnect and retry on the next due entry
        }
    }

    NetworkConfig m_config;
    SPSCQueue<ScheduledCancel, 4096> m_scheduleQueue;
    std::priority_queue<ScheduledCancel, std::vector<ScheduledCancel>, ByCancelTimeAscending> m_heap;
    std::atomic<bool> m_running{ false };
    std::thread m_thread;
    int m_socket{ -1 };
};
