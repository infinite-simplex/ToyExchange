#include "EgressGateway.hpp"
#include "NetworkGateway.hpp"
#include "ReplicaArbiter.hpp"
#include "RetransmitServer.hpp"
#include "OuchProtocolHandler.hpp"
#include "OrderEvent.hpp"
#include "SymbolRegistry.hpp"
#include "OrderTokenRegistry.hpp"
#include "OuchOrderCommand.hpp"
#include "NetworkConfig.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {
    std::atomic<bool> g_shutdownRequested{ false };
    void handleSignal(int) { g_shutdownRequested.store(true); }
}

int main(int argc, char** argv) {
    NetworkConfig config;
    std::string sequenceStorePath = "network_sequence_store.dat";
    size_t sequenceStoreCapacity = 1'048'576; // 2^20 slots
    std::vector<std::string> symbols;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("Missing value for " + arg);
            return argv[++i];
        };

        if (arg == "--ouch-port") config.ouchListenPort = static_cast<uint16_t>(std::stoi(next()));
        else if (arg == "--multicast-ip") config.multicastIp = next();
        else if (arg == "--multicast-port") config.multicastPort = static_cast<uint16_t>(std::stoi(next()));
        else if (arg == "--retransmit-port") config.retransmitServerPort = static_cast<uint16_t>(std::stoi(next()));
        else if (arg == "--sequence-store") sequenceStorePath = next();
        else if (arg == "--symbol") symbols.push_back(next());
        else if (arg == "--egress-ip") config.egressMulticastIp = next();
        else if (arg == "--egress-port") config.egressMulticastPort = static_cast<uint16_t>(std::stoi(next()));
        else if (arg == "--itch-ip") config.itchMulticastGroup = next();
        else if (arg == "--itch-port") config.itchMulticastPort = static_cast<uint16_t>(std::stoi(next()));
        else if (arg == "--arbitration-port") config.arbitrationPort = static_cast<uint16_t>(std::stoi(next()));
        else {
            std::cerr << "Unknown argument: " << arg << "\n";
            return 1;
        }
    }
    // No --symbol given at all: fall back to a single default so a fresh
    // checkout has at least one tradable symbol out of the box. Any
    // ENTER_ORDER for a symbol never registered here silently stalls
    // (OuchProtocolHandler::validateAndParse returns 0 = "incomplete frame"
    // rather than rejecting it) instead of erroring, so this is easy to miss.
    if (symbols.empty()) symbols.push_back("TEST");

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    // Auto-flush every write — see MatchingService/main.cpp for why.
    std::cout.setf(std::ios_base::unitbuf);

    SymbolRegistry symbolRegistry;
    for (const auto& symbol : symbols) {
        symbolRegistry.registerSymbol(symbol);
    }
    OrderTokenRegistry orderTokenRegistry;
    OuchProtocolHandler handler(symbolRegistry, orderTokenRegistry);

    NetworkGateway<OuchProtocolHandler, OuchOrderCommand> gateway(
        handler, config, sequenceStorePath, sequenceStoreCapacity);

    // Serves gap-fill requests from MatchingService replicas'
    // MulticastIngressReceiver out of the same durable log NetworkGateway
    // is appending to.
    RetransmitServer<OuchOrderCommand> retransmitServer(
        gateway.sequenceStore(), config.retransmitServerPort);

    // Arbitrates which MatchingService replica may currently publish egress
    // (see NetworkService/Arbitration/); EgressGateway fences on its epoch.
    ReplicaArbiter arbiter(config.arbitrationPort);

    EgressGateway<NetworkGateway<OuchProtocolHandler, OuchOrderCommand>, OrderEvent> egressGateway(
        gateway, config, arbiter);

    std::cout << "NetworkService starting: OUCH listen :" << config.ouchListenPort
              << ", replication multicast " << config.multicastIp << ":" << config.multicastPort
              << ", retransmit :" << config.retransmitServerPort
              << ", egress from " << config.egressMulticastIp << ":" << config.egressMulticastPort
              << ", ITCH broadcast " << config.itchMulticastGroup << ":" << config.itchMulticastPort
              << ", arbitration :" << config.arbitrationPort
              << ", symbols:";
    for (const auto& symbol : symbols) std::cout << " " << symbol;
    std::cout << "\n";

    gateway.start();
    retransmitServer.start();
    arbiter.start();
    egressGateway.start();

    while (!g_shutdownRequested.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "NetworkService shutting down...\n";
    egressGateway.stop();
    arbiter.stop();
    retransmitServer.stop();
    gateway.stop();
    return 0;
}
