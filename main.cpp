#include "MatchingService.hpp"
#include "NetworkGateway.hpp"
#include "SPSCProducerPolicy.hpp"
#include <iostream>
#include <random>
#include <fstream>


// Resets or initializes the generator sequence with a deterministic seed
inline void SeedRandomOrderGen(uint64_t seed = 1337) {
    // Shared helper function or static state resetting mechanism
}

static Order GetRandomOrder(uint64_t seed = 0) {
    // Fixed initial seed instead of std::random_device{}()
    static std::mt19937_64 rng(1337);

    // Counter to ensure every generated order has a strictly unique ID
    static OrderId current_id = 1;
    static FirmId order_counter = 1;

    // Define random distributions matching your types
    std::uniform_int_distribution<int> side_dist(0, 1); // 0 = BUY, 1 = SELL
    std::uniform_int_distribution<int> type_dist(0, int(ORDER_TYPE::NONE)-1);
    std::uniform_int_distribution<uint16_t> price_dist(0, 101);
    std::uniform_int_distribution<Quantity> qty_dist(-1, 50000);

    // Map random ints back to your strong enums
    SIDE side = (side_dist(rng) == 0) ? SIDE::BID : SIDE::ASK;
    ORDER_TYPE type = static_cast<ORDER_TYPE>(type_dist(rng));
    Price price = static_cast<Price>(price_dist(rng));
    Quantity qty = qty_dist(rng);

    return Order(current_id++, order_counter++, side, type, price, qty);
}

int main() {
    std::cout << "Starting test..." << std::endl;
    // std::ofstream automatically defaults to std::ios::out | std::ios::trunc
    std::ofstream file("log.txt");

    // Always verify the stream opened correctly
    if (!file.is_open()) {
        std::cerr << "Error: Could not open or create file! Check if directory exists." << std::endl;
        return 1;
    }
    SPSCQueue<OrderEvent, 16384> event_q;
    SPSCQueue<OrderTrace, 16384> trace_q;

    // 1. Instantiate policy with valid queue references
    SPSCProducerPolicy policy{ event_q, trace_q };
    auto LOB = new OrderBook<SPSCProducerPolicy>(policy);

    for (int i = 0; i < 100; ++i) {
        auto o = GetRandomOrder();
        auto now = Clock::now();
        LOB->submit_order(o);
        auto end = Clock::now();
        file << std::chrono::nanoseconds(end - now).count() << "\n";
    }

    file.close();
    std::cout << "All done" << std::endl;
    return 0;
}