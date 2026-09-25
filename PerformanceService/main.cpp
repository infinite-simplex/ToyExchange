#include "Aggregator.hpp"
#include "NetworkConfig.hpp"
#include "TelemetryReceiver.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <stdexcept>
#include <thread>

namespace {
    std::atomic<bool> g_shutdownRequested{ false };
    void handleSignal(int) { g_shutdownRequested.store(true); }
}

int main(int argc, char** argv) {
    NetworkConfig defaults;
    uint16_t listenPort = defaults.performanceServicePort;
    int windowMs = static_cast<int>(Aggregator::WINDOW.count());

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("Missing value for " + arg);
            return argv[++i];
        };

        if (arg == "--listen-port") listenPort = static_cast<uint16_t>(std::stoi(next()));
        else if (arg == "--window-ms") windowMs = std::stoi(next());
        else {
            std::cerr << "Unknown argument: " << arg << "\n";
            return 1;
        }
    }

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    // Auto-flush every write — same reasoning as MatchingService/main.cpp:
    // long-lived process, stdout typically redirected to a log file.
    std::cout.setf(std::ios_base::unitbuf);

    Aggregator aggregator;
    TelemetryReceiver receiver(aggregator, listenPort);

    std::cout << "PerformanceService starting: listening on :" << listenPort
              << ", window=" << windowMs << "ms\n";

    receiver.start();

    const auto window = std::chrono::milliseconds(windowMs);
    while (!g_shutdownRequested.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (aggregator.maybeRollover(std::chrono::steady_clock::now(), window)) {
            aggregator.dumpToConsole();
        }
    }

    std::cout << "PerformanceService shutting down...\n";
    receiver.stop();
    return 0;
}
