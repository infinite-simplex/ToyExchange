#pragma once
// Aeron transport for IngressBench. Compiled only when Aeron is available
// (TOYEXCHANGE_AERON, see Benchmarks/CMakeLists.txt). Experimental: it exists to
// compare an Aeron transport with the hand-written one through the same harness.
// If the comparison favours Aeron, the fan-out and receiver move into Core and
// the settings move into NetworkConfig.
//
// Gateway side: AeronFanOut offers each batch of sequenced records as ONE Aeron
// message on an exclusive (single-writer) publication. Replica side: AeronReceiver
// polls a subscription on the same stream and hands every record to the replica's
// queue through the same PushToInboundQueue the multicast receiver uses.
//
// There is no retransmit server and no gap fill here: Aeron delivers a stream in
// order or reports the image as lost. A record out of sequence is counted as a
// violation and the receiver gives up, as the multicast receiver does on an
// unrecoverable gap.
//
// The media driver is a separate process (aeronmd), started by the caller.

#include "BenchRig.hpp"
#include "InboundQueueWriter.hpp"

#include "Aeron.h"
#include "FragmentAssembler.h"
#include "concurrent/BackOffIdleStrategy.h"

#include <atomic>
#include <cstdio>
#include <emmintrin.h>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace bench {

struct AeronSettings {
    std::string aeronDir;      // the gateway's driver; empty = the client's default directory
    std::vector<std::string> replicaDirs; // one driver per replica, or empty to share aeronDir's
    std::string channel;      // e.g. aeron:ipc or aeron:udp?endpoint=239.255.7.7:40456|interface=172.20.0.2/20
    std::int32_t streamId = 1001;
    std::string idle = "busy"; // receiver poll idle strategy: busy | backoff

    static AeronSettings& Get() {
        static AeronSettings settings;
        return settings;
    }
};

inline std::shared_ptr<aeron::Aeron> ConnectToDriver(const std::string& aeronDir) {
    aeron::Context context;
    if (!aeronDir.empty()) context.aeronDir(aeronDir);
    return aeron::Aeron::connect(context);
}

class AeronFanOut {
public:
    explicit AeronFanOut(const NetworkConfig&) {}

    void start() {
        const AeronSettings& settings = AeronSettings::Get();
        m_aeron = ConnectToDriver(settings.aeronDir);
        const std::int64_t id = m_aeron->addExclusivePublication(settings.channel, settings.streamId);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!(m_publication = m_aeron->findExclusivePublication(id))) {
            if (std::chrono::steady_clock::now() > deadline) {
                throw std::runtime_error("Aeron publication was not created within 10 s");
            }
            std::this_thread::yield();
        }
    }

    void stop() {
        if (m_publication) {
            std::fprintf(stderr, "[aeron] gateway: %llu back-pressure retries, %llu records offered while no replica was connected\n",
                static_cast<unsigned long long>(m_backPressureRetries.load()),
                static_cast<unsigned long long>(m_notConnected.load()));
        }
        m_publication.reset();
        m_aeron.reset();
    }

    void publish(const void* records, size_t bytes) {
        aeron::concurrent::AtomicBuffer buffer(
            static_cast<std::uint8_t*>(const_cast<void*>(records)), bytes);
        while (true) {
            const std::int64_t result = m_publication->offer(buffer, 0, static_cast<aeron::util::index_t>(bytes));
            if (result > 0) return;
            if (result == aeron::BACK_PRESSURED || result == aeron::ADMIN_ACTION) {
                m_backPressureRetries.fetch_add(1, std::memory_order_relaxed);
                _mm_pause();
                continue;
            }
            // Not connected, closed or out of position: nothing was sent. The record is
            // already in the sequence store, so a replica could still recover it.
            m_notConnected.fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }

    bool connected() const { return m_publication && m_publication->isConnected(); }

private:
    std::shared_ptr<aeron::Aeron> m_aeron;
    std::shared_ptr<aeron::ExclusivePublication> m_publication;
    std::atomic<std::uint64_t> m_backPressureRetries{ 0 };
    std::atomic<std::uint64_t> m_notConnected{ 0 };
};

class AeronReceiver final : public ReceiverHandle {
public:
    AeronReceiver(SPSCQueue<BenchCommand>& queue, std::string aeronDir)
        : m_queue(queue), m_aeronDir(std::move(aeronDir)) {}
    ~AeronReceiver() override { stop(); }

    void start() override {
        const AeronSettings& settings = AeronSettings::Get();
        m_aeron = ConnectToDriver(m_aeronDir);
        const std::int64_t id = m_aeron->addSubscription(settings.channel, settings.streamId);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!(m_subscription = m_aeron->findSubscription(id))) {
            if (std::chrono::steady_clock::now() > deadline) {
                throw std::runtime_error("Aeron subscription was not created within 10 s");
            }
            std::this_thread::yield();
        }
        m_busy = settings.idle == "busy";
        m_running.store(true);
        m_thread = std::thread(&AeronReceiver::run, this);
    }

    void stop() override {
        m_running.store(false);
        if (m_thread.joinable()) m_thread.join();
        std::lock_guard<std::mutex> lock(m_subscriptionMutex);
        m_subscription.reset();
        m_aeron.reset();
    }

    bool gaveUp() const override { return m_gaveUp.load(); }
    bool ready() const override {
        std::lock_guard<std::mutex> lock(m_subscriptionMutex);
        return m_subscription && m_subscription->imageCount() > 0;
    }

private:
    using Message = SequencedInboundMessage<BenchCommand>;

    void run() {
        aeron::concurrent::BackoffIdleStrategy backoff;
        aeron::FragmentAssembler assembler(
            [this](aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                   aeron::util::index_t length, aeron::Header&) {
                onMessage(buffer.buffer() + offset, static_cast<size_t>(length));
            });
        auto handler = assembler.handler();

        // run() is the only thread that resets the subscription while it is polling it.
        const std::shared_ptr<aeron::Subscription> subscription = m_subscription;
        while (m_running.load(std::memory_order_relaxed) && !m_gaveUp.load(std::memory_order_relaxed)) {
            const int fragments = subscription->poll(handler, 16);
            if (fragments > 0) {
                if (!m_busy) backoff.reset();
            }
            else if (m_busy) {
                _mm_pause();
            }
            else {
                backoff.idle();
            }
        }

        if (m_gaveUp.load()) {
            // A replica that gives up exits its process, which removes its subscription. Do the
            // same here: on shared memory, a subscription that has stopped polling would hold the
            // publisher back for everyone.
            std::lock_guard<std::mutex> lock(m_subscriptionMutex);
            m_subscription.reset();
        }
    }

    // One Aeron message is a batch of whole records, back to back.
    void onMessage(const std::uint8_t* data, size_t length) {
        if (length % sizeof(Message) != 0) {
            m_gaveUp.store(true);
            return;
        }
        for (size_t offset = 0; offset < length; offset += sizeof(Message)) {
            Message msg{};
            std::memcpy(&msg, data + offset, sizeof(Message));
            if (msg.sequenceNumber != m_expected) {
                std::fprintf(stderr, "[aeron] replica: expected %llu, got %llu\n",
                    static_cast<unsigned long long>(m_expected), static_cast<unsigned long long>(msg.sequenceNumber));
                m_gaveUp.store(true);
                return;
            }
            ++m_expected;
            if (!PushToInboundQueue(m_queue, msg, m_running)) return;
        }
    }

    SPSCQueue<BenchCommand>& m_queue;
    std::string m_aeronDir;
    std::shared_ptr<aeron::Aeron> m_aeron;
    mutable std::mutex m_subscriptionMutex;
    std::shared_ptr<aeron::Subscription> m_subscription;
    std::atomic<bool> m_running{ false };
    std::atomic<bool> m_gaveUp{ false };
    std::thread m_thread;
    std::uint64_t m_expected = 0;
    bool m_busy = true;
};

class AeronRig final : public Rig {
public:
    explicit AeronRig(const Options& opt)
        : m_name(opt.transport), m_cfg(MakeNetworkConfig(opt)),
          m_gateway(m_handler, m_cfg, StorePath(), STORE_SLOTS) {
    }
    const char* name() const override { return m_name.c_str(); }
    void start() override { m_gateway.start(); }
    std::unique_ptr<ReceiverHandle> makeReceiver(SPSCQueue<BenchCommand>& queue) override {
        // Each replica gets its own driver when replica directories are given, as on separate hosts.
        const AeronSettings& settings = AeronSettings::Get();
        const size_t index = m_receivers++;
        return std::make_unique<AeronReceiver>(queue,
            index < settings.replicaDirs.size() ? settings.replicaDirs[index] : settings.aeronDir);
    }
    void stop() override { m_gateway.stop(); }
    bool ready() const override { return m_gateway.fanOut().connected(); }

private:
    static std::string StorePath() {
        const std::string path = "/tmp/toyexchange_ingress_bench.dat";
        std::remove(path.c_str());
        return path;
    }
    std::string m_name;
    NetworkConfig m_cfg;
    FrameHandler m_handler;
    NetworkGateway<FrameHandler, BenchCommand, AeronFanOut> m_gateway;
    size_t m_receivers = 0;
};

inline std::unique_ptr<Rig> MakeAeronRig(const Options& opt) {
    AeronSettings& settings = AeronSettings::Get();
    settings.aeronDir = opt.aeronDir;
    settings.replicaDirs = opt.aeronReplicaDirs;
    settings.idle = opt.aeronIdle;
    settings.channel = opt.aeronChannel;
    if (settings.channel.empty()) {
        if (opt.transport == "aeron-ipc") {
            settings.channel = "aeron:ipc";
        }
        else {
            std::fprintf(stderr, "aeron-udp needs --aeron-channel, e.g. aeron:udp?endpoint=239.255.7.7:40456|interface=<ip>/<prefix>\n");
            std::exit(2);
        }
    }
    return std::make_unique<AeronRig>(opt);
}

} // namespace bench
