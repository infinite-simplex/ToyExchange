#include <gtest/gtest.h>
#include "MatchingService.hpp"
#include "NetworkGateway.hpp"
#include "MulticastIngressReceiver.hpp"
#include "RetransmitServer.hpp"
#include "OuchProtocolHandler.hpp"
#include "SymbolRegistry.hpp"
#include "OrderTokenRegistry.hpp"
#include "GtdCancelService.hpp"
#include "NetworkConfig.hpp"
#include "OuchOrderCommand.hpp"
#include <IGoodTillDayScheduler.hpp>
#include <vector>
#include <cstring>
#include <chrono>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

// Type-safe constants instead of macro defines
constexpr uint64_t FIRM_A = 100;
constexpr uint64_t FIRM_B = 99;
constexpr uint64_t FIRM_C = 77;

class MatchingEngineTest : public ::testing::Test {
protected:
    std::vector<OrderEvent> events;
    std::vector<OrderTrace> traces;
    TestingPolicy policy{ events, traces };
    OrderBook<TestingPolicy> LOB{ policy };

    void SetUp() override {
        events.clear();
        traces.clear();
    }
};

// =====================================================================
// LIMIT orders — empty / partial / full book behavior
// =====================================================================

TEST_F(MatchingEngineTest, EmptyBookRestingEmitsSingleAcceptedEvent) {
    Order bid(1, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(bid);

    ASSERT_EQ(size_t(1), events.size());
    EXPECT_EQ(OrderEventType::ACCEPTED, events[0].type);
    EXPECT_EQ(bid.get_id(), events[0].order_id);
    EXPECT_EQ(FIRM_A, events[0].firm_id);
    EXPECT_EQ(SIDE::BID, events[0].side);
    EXPECT_EQ(Price(50), events[0].price);
    EXPECT_EQ(Quantity(100), events[0].quantity);
    EXPECT_EQ(Quantity(100), events[0].leaves_quantity);

    EXPECT_EQ(Price(50), LOB.get_best_bid());
    EXPECT_EQ(Quantity(100), LOB.get_bid_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_ask_quantity());
}

TEST_F(MatchingEngineTest, FIFOTimePriorityOnBidSideAtSamePriceProducesTwoFillPairs) {
    Order bid1(1, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    Order bid2(2, FIRM_B, SIDE::BID, ORDER_TYPE::LIMIT, 50, 200);
    LOB.submit_order(bid1);
    LOB.submit_order(bid2);

    Order ask(3, FIRM_C, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 250);
    LOB.submit_order(ask);

    // Two resting orders touched (bid1 then bid2) -> 2 fill pairs -> 4 EXECUTED events
    // Filter out initial ACCEPTED events if present in the event trace
    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(4), fills.size());

    // Fill 1: ask vs bid1 (fully consumes bid1)
    EXPECT_EQ(bid1.get_id(), fills[0].order_id);
    EXPECT_EQ(FIRM_A, fills[0].firm_id);
    EXPECT_EQ(Quantity(100), fills[0].quantity);
    EXPECT_EQ(Quantity(0), fills[0].leaves_quantity);

    EXPECT_EQ(ask.get_id(), fills[1].order_id);
    EXPECT_EQ(FIRM_C, fills[1].firm_id);
    EXPECT_EQ(Quantity(100), fills[1].quantity);
    EXPECT_EQ(Quantity(150), fills[1].leaves_quantity);

    // Fill 2: ask vs bid2 (leaves bid2 with 50 resting)
    EXPECT_EQ(bid2.get_id(), fills[2].order_id);
    EXPECT_EQ(Quantity(150), fills[2].quantity);
    EXPECT_EQ(Quantity(50), fills[2].leaves_quantity);

    EXPECT_EQ(ask.get_id(), fills[3].order_id);
    EXPECT_EQ(Quantity(150), fills[3].quantity);
    EXPECT_EQ(Quantity(0), fills[3].leaves_quantity);

    EXPECT_EQ(Quantity(50), LOB.get_bid_quantity());
    EXPECT_EQ(Price(50), LOB.get_best_bid());
}

TEST_F(MatchingEngineTest, SweepAcrossThreeLevelsWithFinalPartialFillProducesThreeFillPairs) {
    Order ask1(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    Order ask2(2, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 51, 100);
    Order ask3(3, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 52, 100);
    LOB.submit_order(ask1);
    LOB.submit_order(ask2);
    LOB.submit_order(ask3);

    Order bid(4, FIRM_B, SIDE::BID, ORDER_TYPE::LIMIT, 52, 250);
    LOB.submit_order(bid);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(6), fills.size()); // 3 resting orders touched -> 3 pairs

    EXPECT_EQ(ask1.get_id(), fills[0].order_id);
    EXPECT_EQ(Price(50), fills[0].price);
    EXPECT_EQ(Quantity(100), fills[0].quantity);

    EXPECT_EQ(ask2.get_id(), fills[2].order_id);
    EXPECT_EQ(Price(51), fills[2].price);
    EXPECT_EQ(Quantity(100), fills[2].quantity);

    EXPECT_EQ(ask3.get_id(), fills[4].order_id);
    EXPECT_EQ(Price(52), fills[4].price);
    EXPECT_EQ(Quantity(50), fills[4].quantity);
    EXPECT_EQ(Quantity(50), fills[4].leaves_quantity); // ask3 has 50 left resting

    // Taker (bid) leg on final trade fully fills remaining bid qty
    EXPECT_EQ(Quantity(0), fills[5].leaves_quantity);

    EXPECT_EQ(Price(52), LOB.get_best_ask());
    EXPECT_EQ(Quantity(50), LOB.get_ask_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, TakerLegPriceReflectsActualTradePriceNotOwnLimit) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(ask);

    Order bid(2, FIRM_B, SIDE::BID, ORDER_TYPE::LIMIT, 60, 100);
    LOB.submit_order(bid);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(2), fills.size());
    EXPECT_EQ(Price(50), fills[0].price);
    EXPECT_EQ(Price(50), fills[1].price);
}

TEST_F(MatchingEngineTest, NonCrossingOrderJustRestsOnBothSides) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 55, 100);
    Order bid(2, FIRM_B, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(ask);
    LOB.submit_order(bid);

    EXPECT_EQ(Price(55), LOB.get_best_ask());
    EXPECT_EQ(Price(50), LOB.get_best_bid());
    EXPECT_EQ(Price(5), LOB.get_bid_ask_spread());
}

// =====================================================================
// CANCEL
// =====================================================================

TEST_F(MatchingEngineTest, CancelRestingAskEmitsCanceledEventAndRemovesItFromBook) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(ask);
    ASSERT_EQ(Price(50), LOB.get_best_ask());

    events.clear(); // isolate cancel assertions

    Order cancelAsk(1, FIRM_A, SIDE::ASK, ORDER_TYPE::CANCEL, 0, 0);
    LOB.submit_order(cancelAsk);

    ASSERT_EQ(size_t(1), events.size());
    EXPECT_EQ(OrderEventType::CANCELED, events[0].type);
    EXPECT_EQ(ask.get_id(), events[0].order_id);
    EXPECT_EQ(FIRM_A, events[0].firm_id);
    EXPECT_EQ(Price(50), events[0].price);
    EXPECT_EQ(Quantity(100), events[0].quantity);
    EXPECT_EQ(Quantity(100), events[0].leaves_quantity);

    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_best_ask());
    EXPECT_EQ(Quantity(0), LOB.get_ask_quantity());
}

TEST_F(MatchingEngineTest, CancelRestingBidEmitsCanceledEventAndRemovesItFromBook) {
    Order bid(1, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(bid);
    ASSERT_EQ(Price(50), LOB.get_best_bid());

    events.clear();

    Order cancelBid(1, FIRM_A, SIDE::BID, ORDER_TYPE::CANCEL, 0, 0);
    LOB.submit_order(cancelBid);

    ASSERT_EQ(size_t(1), events.size());
    EXPECT_EQ(OrderEventType::CANCELED, events[0].type);
    EXPECT_EQ(bid.get_id(), events[0].order_id);

    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_best_bid());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, CancelOneOfTwoOrdersAtSamePriceLeavesTheOtherMatchable) {
    Order ask1(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    Order ask2(2, FIRM_B, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 150);
    LOB.submit_order(ask1);
    LOB.submit_order(ask2);
    ASSERT_EQ(Quantity(250), LOB.get_ask_quantity());

    Order cancelAsk1(1, FIRM_A, SIDE::ASK, ORDER_TYPE::CANCEL, 0, 0);
    LOB.submit_order(cancelAsk1);

    EXPECT_EQ(Price(50), LOB.get_best_ask());
    EXPECT_EQ(Quantity(150), LOB.get_ask_quantity());

    events.clear();

    Order bid(3, FIRM_C, SIDE::BID, ORDER_TYPE::LIMIT, 50, 150);
    LOB.submit_order(bid);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(2), fills.size());
    EXPECT_EQ(ask2.get_id(), fills[0].order_id);
    EXPECT_EQ(FIRM_B, fills[0].firm_id);
    EXPECT_EQ(Quantity(150), fills[0].quantity);
    EXPECT_EQ(Quantity(0), fills[0].leaves_quantity);
}

TEST_F(MatchingEngineTest, CancelingAnUnknownOrderIdIsNowASafeNoOp) {
    Order cancelUnknown(12345, FIRM_A, SIDE::ASK, ORDER_TYPE::CANCEL, 0, 0);
    EXPECT_NO_THROW(LOB.submit_order(cancelUnknown));
    EXPECT_TRUE(events.empty());
}

TEST_F(MatchingEngineTest, CancelingAnAlreadyFullyFilledOrderIsASafeNoOp) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    Order bid(2, FIRM_B, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(ask);
    LOB.submit_order(bid);

    events.clear();

    Order cancelAsk(1, FIRM_A, SIDE::ASK, ORDER_TYPE::CANCEL, 0, 0);
    EXPECT_NO_THROW(LOB.submit_order(cancelAsk));
    EXPECT_TRUE(events.empty());
}

// =====================================================================
// FILL_OR_KILL — all-or-nothing, AT THE ORDER'S OWN LIMIT PRICE OR BETTER
// =====================================================================

TEST_F(MatchingEngineTest, EmptyBookNeverFillsAndDoesNotRest) {
    Order fok(1, FIRM_A, SIDE::BID, ORDER_TYPE::FILL_OR_KILL, 50, 100);
    LOB.submit_order(fok);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::INSUFFICIENT_LIQUIDITY);
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_best_bid());
}

TEST_F(MatchingEngineTest, InsufficientTotalLiquidityKillsWithNoPartialFill) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(ask);

    events.clear();

    Order fokBid(2, FIRM_B, SIDE::BID, ORDER_TYPE::FILL_OR_KILL, 60, 150);
    LOB.submit_order(fokBid);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::INSUFFICIENT_LIQUIDITY);
    EXPECT_EQ(Price(50), LOB.get_best_ask());
    EXPECT_EQ(Quantity(100), LOB.get_ask_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, EnoughTotalQuantityButNotEnoughWithinLimitPriceStillKills) {
    Order ask1(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 50);   // qualifies (<= 52)
    Order ask2(2, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 60, 200);  // does NOT qualify (> 52)
    LOB.submit_order(ask1);
    LOB.submit_order(ask2);

    events.clear();

    Order fokBid(3, FIRM_B, SIDE::BID, ORDER_TYPE::FILL_OR_KILL, 52, 100);
    LOB.submit_order(fokBid);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::INSUFFICIENT_LIQUIDITY);
    EXPECT_EQ(Quantity(250), LOB.get_ask_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, SufficientQualifyingLiquiditySweepsMultipleLevelsCompletely) {
    Order ask1(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 60);
    Order ask2(2, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 52, 60);
    Order ask3(3, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 58, 60);
    LOB.submit_order(ask1);
    LOB.submit_order(ask2);
    LOB.submit_order(ask3);

    events.clear();

    Order fokBid(4, FIRM_B, SIDE::BID, ORDER_TYPE::FILL_OR_KILL, 55, 100);
    LOB.submit_order(fokBid);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(4), fills.size());

    EXPECT_EQ(ask1.get_id(), fills[0].order_id);
    EXPECT_EQ(Price(50), fills[0].price);
    EXPECT_EQ(Quantity(60), fills[0].quantity);
    EXPECT_EQ(Quantity(0), fills[0].leaves_quantity);

    EXPECT_EQ(ask2.get_id(), fills[2].order_id);
    EXPECT_EQ(Price(52), fills[2].price);
    EXPECT_EQ(Quantity(40), fills[2].quantity);
    EXPECT_EQ(Quantity(20), fills[2].leaves_quantity);

    EXPECT_EQ(Quantity(0), fills[3].leaves_quantity);

    EXPECT_EQ(Price(52), LOB.get_best_ask());
    EXPECT_EQ(Quantity(80), LOB.get_ask_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, ExactQuantityMatchAtExactlyTheLimitPriceConsumesLevel) {
    Order bid(1, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 50, 200);
    LOB.submit_order(bid);

    events.clear();

    Order fokAsk(2, FIRM_B, SIDE::ASK, ORDER_TYPE::FILL_OR_KILL, 50, 200);
    LOB.submit_order(fokAsk);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(2), fills.size());
    EXPECT_EQ(Quantity(200), fills[0].quantity);
    EXPECT_EQ(Quantity(0), fills[0].leaves_quantity);
    EXPECT_EQ(Quantity(0), fills[1].leaves_quantity);

    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_best_bid());
}

TEST_F(MatchingEngineTest, DoesNotRestOnAKilledAttempt) {
    Order fok(1, FIRM_A, SIDE::ASK, ORDER_TYPE::FILL_OR_KILL, 50, 100);
    LOB.submit_order(fok);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::INSUFFICIENT_LIQUIDITY);
    EXPECT_EQ(Quantity(0), LOB.get_ask_quantity());
}

// =====================================================================
// IMMEDIATE_OR_CANCEL — take what qualifies at limit-or-better, drop rest
// =====================================================================

TEST_F(MatchingEngineTest, EmptyBookProducesCanceledEventAndDoesNotRestIOCOrder) {
    Order ioc(1, FIRM_A, SIDE::BID, ORDER_TYPE::IMMEDIATE_OR_CANCEL, 50, 100);
    LOB.submit_order(ioc);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::NONE);
    EXPECT_EQ(events.back().type, OrderEventType::CANCELED);
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, PartialQualifyingLiquidityFillsAvailableAndDropsRest) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 60);
    LOB.submit_order(ask);

    events.clear();

    Order iocBid(2, FIRM_B, SIDE::BID, ORDER_TYPE::IMMEDIATE_OR_CANCEL, 55, 200);
    LOB.submit_order(iocBid);

    ASSERT_GE(events.size(), size_t(3));

    // Check cancellation of unfilled portion
    EXPECT_EQ(events.back().reject_reason, RejectReason::NONE);
    EXPECT_EQ(events.back().type, OrderEventType::CANCELED);

    EXPECT_EQ(Quantity(0), LOB.get_ask_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_best_ask());
    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_best_bid());
}

TEST_F(MatchingEngineTest, FullQualifyingLiquidityFillsCompletely) {
    Order bid(1, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(bid);

    events.clear();

    Order iocAsk(2, FIRM_B, SIDE::ASK, ORDER_TYPE::IMMEDIATE_OR_CANCEL, 40, 100);
    LOB.submit_order(iocAsk);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(2), fills.size());
    EXPECT_EQ(Quantity(100), fills[0].quantity);
    EXPECT_EQ(Quantity(0), fills[1].leaves_quantity);
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, StopsAtLimitPriceEvenWithMoreWorsePricedLiquidityAvailable) {
    Order ask1(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 50);
    Order ask2(2, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 60, 500);
    LOB.submit_order(ask1);
    LOB.submit_order(ask2);

    events.clear();

    Order iocBid(3, FIRM_B, SIDE::BID, ORDER_TYPE::IMMEDIATE_OR_CANCEL, 55, 200);
    LOB.submit_order(iocBid);

    EXPECT_EQ(events.back().reject_reason, RejectReason::NONE);
    EXPECT_EQ(events.back().type, OrderEventType::CANCELED);
    EXPECT_EQ(Price(60), LOB.get_best_ask());
    EXPECT_EQ(Quantity(500), LOB.get_ask_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, NoQualifyingLiquidityAtAllProducesCanceledEvent) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 60, 100);
    LOB.submit_order(ask);

    events.clear();

    Order iocBid(2, FIRM_B, SIDE::BID, ORDER_TYPE::IMMEDIATE_OR_CANCEL, 55, 100);
    LOB.submit_order(iocBid);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::NONE);
    EXPECT_EQ(events.back().type, OrderEventType::CANCELED);
    EXPECT_EQ(Quantity(100), LOB.get_ask_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, SweepsMultipleQualifyingLevelsThenDropsRemainder) {
    Order ask1(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 50);
    Order ask2(2, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 51, 50);
    LOB.submit_order(ask1);
    LOB.submit_order(ask2);

    events.clear();

    Order iocBid(3, FIRM_B, SIDE::BID, ORDER_TYPE::IMMEDIATE_OR_CANCEL, 60, 500);
    LOB.submit_order(iocBid);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::NONE);
    EXPECT_EQ(events.back().type, OrderEventType::CANCELED);
    EXPECT_EQ(Quantity(0), LOB.get_ask_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

// =====================================================================
// MARKET — no price limit at all; best-effort sweep, remainder dropped
// =====================================================================

TEST_F(MatchingEngineTest, EmptyBookProducesCanceledEventAndDoesNotRestMarketOrder) {
    Order mkt(1, FIRM_A, SIDE::BID, ORDER_TYPE::MARKET, 0, 100);
    LOB.submit_order(mkt);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::NONE);
    EXPECT_EQ(events.back().type, OrderEventType::CANCELED);
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, PartialLiquidityFillsWhatItCanAndDropsRemainder) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 60);
    LOB.submit_order(ask);

    events.clear();

    Order mktBid(2, FIRM_B, SIDE::BID, ORDER_TYPE::MARKET, 0, 200);
    LOB.submit_order(mktBid);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::NONE);
    EXPECT_EQ(events.back().type, OrderEventType::CANCELED);
    EXPECT_EQ(Quantity(0), LOB.get_ask_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, SweepsRegardlessOfPriceAcrossMultipleLevels) {
    Order ask1(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 50);
    Order ask2(2, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 60, 100);
    LOB.submit_order(ask1);
    LOB.submit_order(ask2);

    events.clear();

    Order mktBid(3, FIRM_B, SIDE::BID, ORDER_TYPE::MARKET, 0, 120);
    LOB.submit_order(mktBid);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(4), fills.size());
    EXPECT_EQ(Price(50), fills[0].price);
    EXPECT_EQ(Quantity(50), fills[0].quantity);
    EXPECT_EQ(Price(60), fills[2].price);
    EXPECT_EQ(Quantity(70), fills[2].quantity);

    EXPECT_EQ(Quantity(30), LOB.get_ask_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, FullLiquidityConsumesEntireBookExactly) {
    Order bid(1, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(bid);

    events.clear();

    Order mktAsk(2, FIRM_B, SIDE::ASK, ORDER_TYPE::MARKET, 0, 100);
    LOB.submit_order(mktAsk);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(2), fills.size());
    EXPECT_EQ(Quantity(100), fills[0].quantity);
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_best_bid());
}

TEST_F(MatchingEngineTest, TakerLegPriceReflectsActualTradePriceNotRepricedSentinel) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(ask);

    events.clear();

    Order mktBid(2, FIRM_B, SIDE::BID, ORDER_TYPE::MARKET, 0, 100);
    LOB.submit_order(mktBid);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(2), fills.size());
    EXPECT_EQ(Price(50), fills[0].price);
    EXPECT_EQ(Price(50), fills[1].price);
}

// =====================================================================
// GOOD_TILL_CANCEL / GOOD_TILL_DAY
// =====================================================================

namespace {

class RecordingGtdScheduler : public IGoodTillDayScheduler {
public:
    struct Recorded {
        char orderToken[14];
        std::chrono::system_clock::time_point cancelAt;
    };
    std::vector<Recorded> calls;

    void schedule_cancel(const char orderToken[14], std::chrono::system_clock::time_point cancelAt) override {
        Recorded r{};
        std::memcpy(r.orderToken, orderToken, 14);
        r.cancelAt = cancelAt;
        calls.push_back(r);
    }
};

OuchOrderCommand make_enter_order_command(OrderId orderId, const char orderToken[14], char side,
    Quantity shares, Price price, ORDER_TYPE orderType) {
    OuchOrderCommand cmd{};
    cmd.trace_id = static_cast<TraceId>(orderId);
    cmd.orderId = orderId;
    cmd.price = price;
    cmd.shares = shares;
    cmd.stockLocate = 0;
    std::memcpy(cmd.orderToken, orderToken, 14);
    cmd.buySellIndicator = side;
    cmd.type = CommandType::ENTER_ORDER;
    cmd.orderType = orderType;
    return cmd;
}

} // namespace

class GoodTillDayTest : public ::testing::Test {
protected:
    std::vector<OrderEvent> events;
    std::vector<OrderTrace> traces;
    TestingPolicy policy{ events, traces };
    RecordingGtdScheduler scheduler;
    OrderBook<TestingPolicy> LOB{ policy, &scheduler };
};

TEST_F(GoodTillDayTest, GoodTillCancelRestsWithoutSchedulingAutoCancel) {
    auto cmd = make_enter_order_command(1, "GTC-TOKEN0001", 'B', 100, 50, ORDER_TYPE::GOOD_TILL_CANCEL);
    LOB.submit_order(cmd);

    ASSERT_EQ(size_t(1), events.size());
    EXPECT_EQ(OrderEventType::ACCEPTED, events[0].type);
    EXPECT_EQ(Price(50), LOB.get_best_bid());
    EXPECT_TRUE(scheduler.calls.empty());
}

TEST_F(GoodTillDayTest, GoodTillDayRestsAndSchedulesAutoCancelAtFourPmEst) {
    const char token[14] = "GTD-TOKEN0001";
    auto cmd = make_enter_order_command(1, token, 'B', 100, 50, ORDER_TYPE::GOOD_TILL_DAY);

    auto before = today_4pm_est_utc();
    LOB.submit_order(cmd);
    auto after = today_4pm_est_utc();

    ASSERT_EQ(size_t(1), events.size());
    EXPECT_EQ(OrderEventType::ACCEPTED, events[0].type);
    EXPECT_EQ(Price(50), LOB.get_best_bid());

    ASSERT_EQ(size_t(1), scheduler.calls.size());
    EXPECT_EQ(0, std::memcmp(token, scheduler.calls[0].orderToken, 14));
    EXPECT_GE(scheduler.calls[0].cancelAt, before);
    EXPECT_LE(scheduler.calls[0].cancelAt, after);
}

TEST_F(GoodTillDayTest, GoodTillDayThatFullyFillsOnEntryDoesNotScheduleAutoCancel) {
    // Resting leg entered directly as an Order with a real firm id, so it
    // doesn't self-trade-prevent against the GTD command below (which is
    // hardcoded to firm 0 by OrderBook::submit_order(OuchOrderCommand&)).
    Order restingAsk(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(restingAsk);

    auto cmd = make_enter_order_command(2, "GTD-TOKEN0002", 'B', 100, 50, ORDER_TYPE::GOOD_TILL_DAY);
    LOB.submit_order(cmd);

    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_best_bid()); // nothing rested — fully crossed
    EXPECT_TRUE(scheduler.calls.empty());
}

// =====================================================================
// Price bounds validation
// =====================================================================

TEST_F(MatchingEngineTest, OutOfRangeWirePriceIsRejectedNotIndexed) {
    // Simulates a corrupted/malicious wire price (e.g. 105) narrowed into the
    // uint8_t Price field by OuchProtocolHandler: must be rejected rather than
    // used to index BookSide::m_price_levels (size MAX_LEVELS=101) out of bounds.
    auto cmd = make_enter_order_command(1, "BAD-PRICE001", 'S', 100, 105, ORDER_TYPE::LIMIT);
    LOB.submit_order(cmd);

    ASSERT_EQ(size_t(1), events.size());
    EXPECT_EQ(OrderEventType::REJECTED, events[0].type);
    EXPECT_EQ(RejectReason::PRICE_OUT_OF_RANGE, events[0].reject_reason);
    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_best_ask());
    EXPECT_EQ(Quantity(0), LOB.get_ask_quantity());
}

TEST_F(MatchingEngineTest, MaxUint8WirePriceIsRejectedNotIndexed) {
    auto cmd = make_enter_order_command(1, "BAD-PRICE255", 'B', 100, 255, ORDER_TYPE::LIMIT);
    LOB.submit_order(cmd);

    ASSERT_EQ(size_t(1), events.size());
    EXPECT_EQ(OrderEventType::REJECTED, events[0].type);
    EXPECT_EQ(RejectReason::PRICE_OUT_OF_RANGE, events[0].reject_reason);
    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_best_bid());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

// =====================================================================
// Modify Trade Type
// =====================================================================

TEST_F(MatchingEngineTest, ModifyProducesCancelAndAdd) {
    Order limit(1, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(limit);
    EXPECT_EQ(Quantity(100), LOB.get_bid_quantity());
    EXPECT_EQ(Price(50), LOB.get_best_bid());

    events.clear();

    Order modify(1, FIRM_A, SIDE::BID, ORDER_TYPE::MODIFY, 60, 50);
    LOB.submit_order(modify);

    ASSERT_EQ(2, events.size());
    EXPECT_EQ(OrderEventType::CANCELED, events[0].type);
    EXPECT_EQ(OrderEventType::ACCEPTED, events[1].type);
    EXPECT_EQ(Price(60), LOB.get_best_bid());
    EXPECT_EQ(Quantity(50), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, ModifyChangesQueuePosition) {
    Order limit(1, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 50, 40);
    Order limit2(2, FIRM_B, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    Order modify(1, FIRM_A, SIDE::BID, ORDER_TYPE::MODIFY, 50, 30);
    LOB.submit_order(limit);
    LOB.submit_order(limit2);
    LOB.submit_order(modify);

    EXPECT_EQ(Quantity(130), LOB.get_bid_quantity());
    EXPECT_EQ(Price(50), LOB.get_best_bid());

    events.clear();

    Order marketOrder(4, FIRM_C, SIDE::ASK, ORDER_TYPE::MARKET, 50, 50);
    LOB.submit_order(marketOrder);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_GE(fills.size(), size_t(1));
    EXPECT_EQ(2, fills[0].order_id); // Order 2 was prioritized over modified Order 1
}

// =====================================================================
// Self-trade prevention
// =====================================================================

TEST_F(MatchingEngineTest, SameFirmLimitOrdersProduceRejectReasonAndDoNotTouchEitherOrder) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(ask);

    events.clear();

    Order bid(2, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(bid);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::SELF_TRADING_PREVENTION);

    EXPECT_EQ(Price(50), LOB.get_best_ask());
    EXPECT_EQ(Quantity(100), LOB.get_ask_quantity());
    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_best_bid());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, DifferentFirmsStillTradeNormally) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(ask);

    events.clear();

    Order bid(2, FIRM_B, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(bid);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(2), fills.size());
    EXPECT_EQ(ask.get_id(), fills[0].order_id);
    EXPECT_EQ(FIRM_A, fills[0].firm_id);
    EXPECT_EQ(bid.get_id(), fills[1].order_id);
    EXPECT_EQ(FIRM_B, fills[1].firm_id);
}

TEST_F(MatchingEngineTest, EarlierFillsAgainstOtherFirmsStillFireBeforeHittingOwnOrder) {
    Order otherFirmAsk(1, FIRM_B, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 40);
    Order ownFirmAsk(2, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 60);
    LOB.submit_order(otherFirmAsk);
    LOB.submit_order(ownFirmAsk);

    events.clear();

    Order bid(3, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(bid);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_GE(fills.size(), size_t(1));
    EXPECT_EQ(otherFirmAsk.get_id(), fills[0].order_id);
    EXPECT_EQ(Quantity(40), fills[0].quantity);

    EXPECT_EQ(events.back().reject_reason, RejectReason::SELF_TRADING_PREVENTION);

    EXPECT_EQ(Quantity(60), LOB.get_ask_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

TEST_F(MatchingEngineTest, SameFirmFillOrKillDoesNotSelfMatchWhenItsTheOnlyLiquidity) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(ask);

    events.clear();

    Order fokBid(2, FIRM_A, SIDE::BID, ORDER_TYPE::FILL_OR_KILL, 50, 100);
    LOB.submit_order(fokBid);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::INSUFFICIENT_LIQUIDITY);
    EXPECT_EQ(Quantity(100), LOB.get_ask_quantity());
}

TEST_F(MatchingEngineTest, SameFirmMarketOrderDoesNotSelfMatch) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(ask);

    events.clear();

    Order mktBid(2, FIRM_A, SIDE::BID, ORDER_TYPE::MARKET, 0, 100);
    LOB.submit_order(mktBid);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::SELF_TRADING_PREVENTION);
    EXPECT_EQ(Quantity(100), LOB.get_ask_quantity());
}

TEST_F(MatchingEngineTest, PreflightCheckIgnoresOwnFirmAndCanPartiallyFill) {
    Order otherFirmAsk(1, FIRM_B, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 40);
    Order ownFirmAsk(2, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 60);
    LOB.submit_order(otherFirmAsk);
    LOB.submit_order(ownFirmAsk);

    events.clear();

    Order fokBid(3, FIRM_A, SIDE::BID, ORDER_TYPE::FILL_OR_KILL, 55, 100);
    LOB.submit_order(fokBid);

    ASSERT_FALSE(events.empty());
    EXPECT_EQ(events.back().reject_reason, RejectReason::INSUFFICIENT_LIQUIDITY);
    EXPECT_EQ(Quantity(100), LOB.get_ask_quantity());
    EXPECT_EQ(Quantity(0), LOB.get_bid_quantity());
}

// =====================================================================
// Spread / quantity helpers
// =====================================================================

TEST_F(MatchingEngineTest, ReturnsZeroWhenAskSideIsEmpty) {
    Order bid(1, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(bid);
    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_bid_ask_spread());
}

TEST_F(MatchingEngineTest, ReturnsZeroWhenBidSideIsEmpty) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(ask);
    EXPECT_EQ(Price(INVALID_PRICE), LOB.get_bid_ask_spread());
}

TEST_F(MatchingEngineTest, ComputesPositiveSpreadWithBothSidesResting) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 60, 100);
    Order bid(2, FIRM_B, SIDE::BID, ORDER_TYPE::LIMIT, 45, 100);
    LOB.submit_order(ask);
    LOB.submit_order(bid);
    EXPECT_EQ(Price(15), LOB.get_bid_ask_spread());
}

TEST_F(MatchingEngineTest, TreatsAGenuineRestingBidAtPriceZeroAsIfTheBookWereEmpty) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 10, 100);
    Order bid(2, FIRM_B, SIDE::BID, ORDER_TYPE::LIMIT, 0, 100);
    LOB.submit_order(ask);
    LOB.submit_order(bid);

    ASSERT_EQ(Price(0), LOB.get_best_bid());
    EXPECT_EQ(Price(10), LOB.get_bid_ask_spread());
}

// =====================================================================
// Generated match_id / sequence_number assertions
// =====================================================================

TEST_F(MatchingEngineTest, BothLegsOfOneFillShareTheSameMatchId) {
    Order ask(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(ask);

    events.clear();

    Order bid(2, FIRM_B, SIDE::BID, ORDER_TYPE::LIMIT, 50, 100);
    LOB.submit_order(bid);

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(2), fills.size());
    EXPECT_EQ(fills[0].match_id, fills[1].match_id);
    EXPECT_NE(fills[0].match_id, 0);
}

TEST_F(MatchingEngineTest, DifferentTradesGetDifferentMatchIds) {
    Order ask1(1, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 50, 50);
    Order bid1(2, FIRM_B, SIDE::BID, ORDER_TYPE::LIMIT, 50, 50);
    Order ask2(3, FIRM_A, SIDE::ASK, ORDER_TYPE::LIMIT, 60, 50);
    Order bid2(4, FIRM_B, SIDE::BID, ORDER_TYPE::LIMIT, 60, 50);

    LOB.submit_order(ask1);
    LOB.submit_order(bid1); // trade 1
    LOB.submit_order(ask2);
    LOB.submit_order(bid2); // trade 2

    std::vector<OrderEvent> fills;
    for (const auto& ev : events) {
        if (ev.type == OrderEventType::EXECUTED) {
            fills.push_back(ev);
        }
    }

    ASSERT_EQ(size_t(4), fills.size());
    EXPECT_NE(fills[0].match_id, fills[2].match_id);
}

TEST_F(MatchingEngineTest, SequenceNumbersAreMonotonicallyIncreasing) {
    Order bid1(1, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 50, 10);
    Order bid2(2, FIRM_A, SIDE::BID, ORDER_TYPE::LIMIT, 49, 10);
    LOB.submit_order(bid1);
    LOB.submit_order(bid2);

    ASSERT_GE(events.size(), size_t(2));
    for (size_t i = 1; i < events.size(); ++i) {
        EXPECT_GT(events[i].sequence_number, events[i - 1].sequence_number);
    }
}

// =====================================================================
// NetworkGateway / MulticastIngressReceiver end-to-end ingress pipeline
// =====================================================================

namespace {

// Builds an OUCH ENTER_ORDER ('O') frame matching OuchProtocolHandler's
// expected wire layout: [2B BE payloadLen][1B 'O'][14B token][1B side]
// [1B orderType][4B BE shares][8B symbol][4B BE price][4B BE firmId].
// firmId defaults to 0 so existing call sites (which predate firmId and
// implicitly rely on every OUCH order sharing the same firm) don't need to
// change; pass distinct firmIds to exercise cross-firm matching instead of
// tripping self-trade prevention.
std::vector<char> BuildOuchEnterOrderFrame(const char orderToken[14], char buySellIndicator,
    uint32_t shares, const char symbol[8], uint32_t price, ORDER_TYPE orderType = ORDER_TYPE::LIMIT,
    uint32_t firmId = 0) {
    constexpr size_t bodyLen = 14 + 1 + 1 + 4 + 8 + 4 + 4;
    constexpr uint16_t payloadLen = static_cast<uint16_t>(1 + bodyLen);

    std::vector<char> frame(2 + payloadLen);
    size_t off = 0;
    frame[off++] = static_cast<char>((payloadLen >> 8) & 0xFF);
    frame[off++] = static_cast<char>(payloadLen & 0xFF);
    frame[off++] = 'O';
    std::memcpy(frame.data() + off, orderToken, 14); off += 14;
    frame[off++] = buySellIndicator;
    frame[off++] = static_cast<char>(orderType);
    frame[off++] = static_cast<char>((shares >> 24) & 0xFF);
    frame[off++] = static_cast<char>((shares >> 16) & 0xFF);
    frame[off++] = static_cast<char>((shares >> 8) & 0xFF);
    frame[off++] = static_cast<char>(shares & 0xFF);
    std::memcpy(frame.data() + off, symbol, 8); off += 8;
    frame[off++] = static_cast<char>((price >> 24) & 0xFF);
    frame[off++] = static_cast<char>((price >> 16) & 0xFF);
    frame[off++] = static_cast<char>((price >> 8) & 0xFF);
    frame[off++] = static_cast<char>(price & 0xFF);
    frame[off++] = static_cast<char>((firmId >> 24) & 0xFF);
    frame[off++] = static_cast<char>((firmId >> 16) & 0xFF);
    frame[off++] = static_cast<char>((firmId >> 8) & 0xFF);
    frame[off++] = static_cast<char>(firmId & 0xFF);
    return frame;
}

// Busy-polls try_pop until an item is available or the timeout elapses.
// Returns false (rather than hanging) if nothing arrives in time.
template <typename TCommand, size_t Capacity>
bool WaitForPop(SPSCQueue<TCommand, Capacity>& queue, TCommand& out,
    std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (queue.try_pop(out)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return false;
}

} // namespace

TEST(NetworkIngressPipelineTest, GatewayToReceiverDeliversParsedOrderOverLoopback) {
    SymbolRegistry registry;
    uint16_t expectedStockLocate = registry.registerSymbol("TEST");
    OrderTokenRegistry orderTokenRegistry;
    OuchProtocolHandler handler(registry, orderTokenRegistry);

    NetworkConfig config;
    config.ouchListenPort = 15101;
    config.multicastIp = "239.255.0.1";
    config.multicastPort = 25101;

    NetworkGateway<OuchProtocolHandler, OuchOrderCommand> gateway(
        handler, config, "/tmp/eceo_test_seq_store_gw.dat", 1024);
    gateway.start();

    SPSCQueue<OuchOrderCommand> inboundQueue;
    MulticastIngressReceiver<OuchOrderCommand> receiver(inboundQueue, config);
    receiver.start();

    // Give the listen socket / multicast group join a moment to complete.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int clientFd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(clientFd, 0);
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(config.ouchListenPort);
    inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);
    ASSERT_EQ(0, connect(clientFd, (sockaddr*)&serverAddr, sizeof(serverAddr)));

    const char orderToken[14] = "TESTTOKEN0001";
    const char symbol[8] = { 'T', 'E', 'S', 'T', ' ', ' ', ' ', ' ' };
    auto frame = BuildOuchEnterOrderFrame(orderToken, 'B', 100, symbol, 42, ORDER_TYPE::LIMIT, 7);
    ASSERT_EQ(static_cast<ssize_t>(frame.size()),
        send(clientFd, frame.data(), frame.size(), 0));
    close(clientFd);

    OuchOrderCommand received{};
    ASSERT_TRUE(WaitForPop(inboundQueue, received))
        << "Order never arrived at MulticastIngressReceiver's queue";

    EXPECT_EQ(CommandType::ENTER_ORDER, received.type);
    EXPECT_EQ('B', received.buySellIndicator);
    EXPECT_EQ(Quantity(100), received.shares);
    EXPECT_EQ(Price(42), received.price);
    EXPECT_EQ(expectedStockLocate, received.stockLocate);
    EXPECT_EQ(FirmId(7), received.firmId);
    EXPECT_EQ(0, std::memcmp(orderToken, received.orderToken, 14));

    receiver.stop();
    gateway.stop();
}

TEST(NetworkIngressPipelineTest, DistinctFirmIdsAllowOuchOrdersToMatch) {
    SymbolRegistry registry;
    registry.registerSymbol("TEST");
    OrderTokenRegistry orderTokenRegistry;
    OuchProtocolHandler handler(registry, orderTokenRegistry);

    NetworkConfig config;
    config.ouchListenPort = 15105;
    config.multicastIp = "239.255.0.5";
    config.multicastPort = 25105;

    NetworkGateway<OuchProtocolHandler, OuchOrderCommand> gateway(
        handler, config, "/tmp/eceo_test_seq_store_firmid.dat", 1024);
    gateway.start();

    SPSCQueue<OuchOrderCommand> inboundQueue;
    MulticastIngressReceiver<OuchOrderCommand> receiver(inboundQueue, config);
    receiver.start();

    SPSCQueue<OrderEvent, 16384> eventQ;
    SPSCQueue<OrderTrace, 16384> traceQ;
    SPSCProducerPolicy policy{ eventQ, traceQ };
    OrderBook<SPSCProducerPolicy> lob{ policy };

    MatchingService<OuchOrderCommand> matchingService(inboundQueue, lob);
    matchingService.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto sendFrame = [&](const std::vector<char>& frame) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in serverAddr{};
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(config.ouchListenPort);
        inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);
        ASSERT_EQ(0, connect(fd, (sockaddr*)&serverAddr, sizeof(serverAddr)));
        ASSERT_EQ(static_cast<ssize_t>(frame.size()), send(fd, frame.data(), frame.size(), 0));
        close(fd);
    };

    const char symbol[8] = { 'T', 'E', 'S', 'T', ' ', ' ', ' ', ' ' };
    const char askToken[14] = "FIRMIDASK0001";
    const char bidToken[14] = "FIRMIDBID0001";

    // Resting ask from firm 1, crossed by a bid from firm 2 — distinct firm
    // ids, so self-trade prevention must not block this (see OuchOrderCommand
    // ::firmId and OrderBook::create_order_from_command).
    sendFrame(BuildOuchEnterOrderFrame(askToken, 'S', 50, symbol, 40, ORDER_TYPE::LIMIT, 1));
    sendFrame(BuildOuchEnterOrderFrame(bidToken, 'B', 50, symbol, 40, ORDER_TYPE::LIMIT, 2));

    OrderEvent evt{};
    bool sawExecuted = false;
    for (int i = 0; i < 4 && !sawExecuted; ++i) {
        ASSERT_TRUE(WaitForPop(eventQ, evt)) << "Expected events never arrived";
        if (evt.type == OrderEventType::EXECUTED) sawExecuted = true;
    }
    EXPECT_TRUE(sawExecuted) << "Distinct-firm orders should match instead of "
                                 "hitting self-trade prevention";

    matchingService.stop();
    receiver.stop();
    gateway.stop();
}

TEST(NetworkIngressPipelineTest, ReceiverRecoversGapViaRetransmitServer) {
    SequenceStore<OuchOrderCommand> store("/tmp/eceo_test_seq_store_retransmit.dat", 1024);

    SequencedInboundMessage<OuchOrderCommand> msg0{};
    msg0.sequenceNumber = 0;
    msg0.ingressTsc = 1;
    msg0.clientSessionId = 1;
    msg0.command.trace_id = 500;
    msg0.command.type = CommandType::ENTER_ORDER;
    msg0.command.shares = 111;
    msg0.command.price = 10;
    msg0.command.buySellIndicator = 'B';
    std::memcpy(msg0.command.orderToken, "MISSING-ORDER0", 14);
    store.append(msg0);

    SequencedInboundMessage<OuchOrderCommand> msg1{};
    msg1.sequenceNumber = 1;
    msg1.ingressTsc = 2;
    msg1.clientSessionId = 1;
    msg1.command.trace_id = 501;
    msg1.command.type = CommandType::ENTER_ORDER;
    msg1.command.shares = 222;
    msg1.command.price = 20;
    msg1.command.buySellIndicator = 'S';
    std::memcpy(msg1.command.orderToken, "LIVE-ORDER0001", 14);
    store.append(msg1);

    RetransmitServer<OuchOrderCommand> retransmitServer(store, 45102);
    retransmitServer.start();

    NetworkConfig config;
    config.multicastIp = "239.255.0.2";
    config.multicastPort = 25102;
    config.retransmitServerIp = "127.0.0.1";
    config.retransmitServerPort = 45102;

    SPSCQueue<OuchOrderCommand> inboundQueue;
    MulticastIngressReceiver<OuchOrderCommand> receiver(inboundQueue, config);
    receiver.start();

    // Give the multicast group join a moment to complete.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Simulate NetworkGateway's fan-out, but skip seq 0 to force a gap:
    // the receiver expects 0, sees 1 arrive live, and must recover 0 itself.
    int senderFd = socket(AF_INET, SOCK_DGRAM, 0);
    ASSERT_GE(senderFd, 0);
    sockaddr_in destAddr{};
    destAddr.sin_family = AF_INET;
    destAddr.sin_port = htons(config.multicastPort);
    inet_pton(AF_INET, config.multicastIp.c_str(), &destAddr.sin_addr);
    ASSERT_EQ(static_cast<ssize_t>(sizeof(msg1)),
        sendto(senderFd, &msg1, sizeof(msg1), 0, (sockaddr*)&destAddr, sizeof(destAddr)));
    close(senderFd);

    OuchOrderCommand first{};
    ASSERT_TRUE(WaitForPop(inboundQueue, first))
        << "Recovered (gap-filled) order never arrived";
    EXPECT_EQ(Quantity(111), first.shares) << "Expected the retransmit-recovered seq-0 order first";

    OuchOrderCommand second{};
    ASSERT_TRUE(WaitForPop(inboundQueue, second))
        << "Live order never arrived after gap recovery";
    EXPECT_EQ(Quantity(222), second.shares) << "Expected the live seq-1 order second";

    receiver.stop();
    retransmitServer.stop();
}

TEST(NetworkIngressPipelineTest, TelemetryIsCapturedConsistentlyAcrossStages) {
    SymbolRegistry registry;
    registry.registerSymbol("TEST");
    OrderTokenRegistry orderTokenRegistry;
    OuchProtocolHandler handler(registry, orderTokenRegistry);

    NetworkConfig config;
    config.ouchListenPort = 15103;
    config.multicastIp = "239.255.0.3";
    config.multicastPort = 25103;

    NetworkGateway<OuchProtocolHandler, OuchOrderCommand> gateway(
        handler, config, "/tmp/eceo_test_seq_store_telemetry.dat", 1024);
    gateway.start();

    SPSCQueue<OuchOrderCommand> inboundQueue;
    MulticastIngressReceiver<OuchOrderCommand> receiver(inboundQueue, config);
    receiver.start();

    // Real (thread-safe) producer policy, since MatchingService runs on its
    // own thread and on_trace_complete()/on_order_event() fire from there —
    // TestingPolicy's plain std::vectors aren't safe to share across threads.
    SPSCQueue<OrderEvent, 16384> eventQ;
    SPSCQueue<OrderTrace, 16384> traceQ;
    SPSCProducerPolicy policy{ eventQ, traceQ };
    OrderBook<SPSCProducerPolicy> lob{ policy };

    MatchingService<OuchOrderCommand> matchingService(inboundQueue, lob);
    matchingService.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int clientFd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(clientFd, 0);
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(config.ouchListenPort);
    inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);
    ASSERT_EQ(0, connect(clientFd, (sockaddr*)&serverAddr, sizeof(serverAddr)));

    // NOTE: on_trace_complete only fires when an order is *evicted* from the
    // book (fully filled or canceled); a lone resting ACCEPTED order never
    // triggers it. So this test reads the arena directly rather than relying
    // on eviction: this is the only order NetworkGateway will parse in this
    // test, so its trace_id is deterministically sequence 0.
    const char orderToken[14] = "TELEMTOKEN001";
    const char symbol[8] = { 'T', 'E', 'S', 'T', ' ', ' ', ' ', ' ' };
    auto frame = BuildOuchEnterOrderFrame(orderToken, 'B', 50, symbol, 30);
    ASSERT_EQ(static_cast<ssize_t>(frame.size()),
        send(clientFd, frame.data(), frame.size(), 0));
    close(clientFd);

    OrderEvent accepted{};
    ASSERT_TRUE(WaitForPop(eventQ, accepted)) << "Order was never accepted by the book";

    OrderTrace& trace = g_telemetry_arena[trace_index(0)];
    // match_done_tsc is written just after submit_order() returns, which can
    // land a hair after the ACCEPTED event popped above — give it a moment.
    for (int i = 0; i < 200 && trace.match_done_tsc == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    EXPECT_NE(0u, trace.ingress_tsc) << "NetworkGateway's socket-read tsc never made it into the arena";
    EXPECT_NE(0u, trace.engine_pop_tsc) << "MatchingService's queue-pop tsc never made it into the arena";
    EXPECT_NE(0u, trace.match_done_tsc) << "MatchingService's post-submit tsc never made it into the arena";
    EXPECT_GE(trace.match_done_tsc, trace.engine_pop_tsc)
        << "match_done_tsc and engine_pop_tsc are both stamped from the same thread in sequence";

    matchingService.stop();
    receiver.stop();
    gateway.stop();
}

TEST(NetworkIngressPipelineTest, GtdCancelServiceFiresScheduledCancelWithinBoundedWindow) {
    SymbolRegistry registry;
    registry.registerSymbol("TEST");
    OrderTokenRegistry orderTokenRegistry;
    OuchProtocolHandler handler(registry, orderTokenRegistry);

    NetworkConfig config;
    config.ouchListenPort = 15104;
    config.multicastIp = "239.255.0.4";
    config.multicastPort = 25104;

    NetworkGateway<OuchProtocolHandler, OuchOrderCommand> gateway(
        handler, config, "/tmp/eceo_test_seq_store_gtd.dat", 1024);
    gateway.start();

    SPSCQueue<OuchOrderCommand> inboundQueue;
    MulticastIngressReceiver<OuchOrderCommand> receiver(inboundQueue, config);
    receiver.start();

    SPSCQueue<OrderEvent, 16384> eventQ;
    SPSCQueue<OrderTrace, 16384> traceQ;
    SPSCProducerPolicy policy{ eventQ, traceQ };
    OrderBook<SPSCProducerPolicy> lob{ policy };

    MatchingService<OuchOrderCommand> matchingService(inboundQueue, lob);
    matchingService.start();

    // The service itself is just another OUCH TCP client of this same
    // gateway — no wiring to OrderBook's own GTD hook needed for this test
    // (that's covered by GoodTillDayTest); here we exercise its real
    // heap/poll/TCP-send machinery end to end.
    GtdCancelService gtdCancelService(config);
    gtdCancelService.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int clientFd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(clientFd, 0);
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(config.ouchListenPort);
    inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);
    ASSERT_EQ(0, connect(clientFd, (sockaddr*)&serverAddr, sizeof(serverAddr)));

    const char orderToken[14] = "GTDSVCTOKEN01";
    const char symbol[8] = { 'T', 'E', 'S', 'T', ' ', ' ', ' ', ' ' };
    auto frame = BuildOuchEnterOrderFrame(orderToken, 'B', 100, symbol, 40, ORDER_TYPE::GOOD_TILL_CANCEL);
    ASSERT_EQ(static_cast<ssize_t>(frame.size()),
        send(clientFd, frame.data(), frame.size(), 0));
    close(clientFd);

    OrderEvent accepted{};
    ASSERT_TRUE(WaitForPop(eventQ, accepted)) << "Order was never accepted";
    ASSERT_EQ(OrderEventType::ACCEPTED, accepted.type);

    // Bypass OrderBook's own 4pm computation — schedule directly for the near
    // future so the test doesn't wait for a real 4pm.
    gtdCancelService.schedule_cancel(orderToken,
        std::chrono::system_clock::now() + std::chrono::milliseconds(50));

    OrderEvent canceled{};
    ASSERT_TRUE(WaitForPop(eventQ, canceled, std::chrono::milliseconds(500)))
        << "GtdCancelService never fired the scheduled cancel within the bounded window";
    EXPECT_EQ(OrderEventType::CANCELED, canceled.type);
    EXPECT_EQ(accepted.order_id, canceled.order_id);

    gtdCancelService.stop();
    matchingService.stop();
    receiver.stop();
    gateway.stop();
}