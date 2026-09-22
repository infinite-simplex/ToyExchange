#include "EgressPublisher.hpp"
#include "MatchingService.hpp"
#include "MulticastIngressReceiver.hpp"
#include "NetworkConfig.hpp"
#include "OuchOrderCommand.hpp"
#include "OrderBook.hpp"
#include "SPSCProducerPolicy.hpp"
#include "SPSCQueue.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>

namespace {
    std::atomic<bool> g_shutdownRequested{ false };
    void handleSignal(int) { g_shutdownRequested.store(true); }
}

int main(int argc, char** argv) {
    NetworkConfig config;
    std::string replicaId = "replica-1";
    int statsIntervalMs = 0; // 0 = disabled; opt in for local debugging/smoke tests

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("Missing value for " + arg);
            return argv[++i];
        };

        if (arg == "--multicast-ip") config.multicastIp = next();
        else if (arg == "--multicast-port") config.multicastPort = static_cast<uint16_t>(std::stoi(next()));
        else if (arg == "--retransmit-ip") config.retransmitServerIp = next();
        else if (arg == "--retransmit-port") config.retransmitServerPort = static_cast<uint16_t>(std::stoi(next()));
        else if (arg == "--id") replicaId = next();
        else if (arg == "--stats-interval-ms") statsIntervalMs = std::stoi(next());
        else if (arg == "--egress-ip") config.egressMulticastIp = next();
        else if (arg == "--egress-port") config.egressMulticastPort = static_cast<uint16_t>(std::stoi(next()));
        else {
            std::cerr << "Unknown argument: " << arg << "\n";
            return 1;
        }
    }

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    // Auto-flush every write: this process typically runs long-lived with
    // stdout redirected to a log file, and a reader tailing that file while
    // the process is still up should see output as it happens rather than
    // whatever's sitting in a full-buffered stdio buffer.
    std::cout.setf(std::ios_base::unitbuf);

    // Heap-allocated: these ring buffers are too large to sit safely on
    // main's stack frame (same reasoning as OrderTokenRegistry's table).
    // inboundQueue uses SPSCQueue's default capacity (1024) because
    // MatchingService<TCommand> and MulticastIngressReceiver<TCommand> are
    // only templated on TCommand, not on queue capacity, and both hardcode
    // SPSCQueue<TCommand> internally.
    auto inboundQueue = std::make_unique<SPSCQueue<OuchOrderCommand>>();
    auto eventQueue = std::make_unique<SPSCQueue<OrderEvent, 16384>>();
    auto traceQueue = std::make_unique<SPSCQueue<OrderTrace, 16384>>();

    SPSCProducerPolicy policy{ *eventQueue, *traceQueue };
    OrderBook<SPSCProducerPolicy> book(policy);
    MatchingService<OuchOrderCommand> matcher(*inboundQueue, book);
    MulticastIngressReceiver<OuchOrderCommand> receiver(*inboundQueue, config);
    EgressPublisher<OrderEvent, 16384> egressPublisher(*eventQueue, config);

    std::cout << "MatchingService[" << replicaId << "] starting: joining "
              << config.multicastIp << ":" << config.multicastPort
              << ", retransmit via " << config.retransmitServerIp << ":" << config.retransmitServerPort
              << ", egress to " << config.egressMulticastIp << ":" << config.egressMulticastPort << "\n";

    receiver.start();
    matcher.start();
    egressPublisher.start();

    if (statsIntervalMs > 0) {
        while (!g_shutdownRequested.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(statsIntervalMs));
            std::cout << "[" << replicaId << "] best bid=" << static_cast<int>(book.get_best_bid())
                      << " best ask=" << static_cast<int>(book.get_best_ask()) << "\n";
        }
    } else {
        while (!g_shutdownRequested.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }

    std::cout << "MatchingService[" << replicaId << "] shutting down...\n";
    egressPublisher.stop();
    matcher.stop();
    receiver.stop();
    return 0;
}
