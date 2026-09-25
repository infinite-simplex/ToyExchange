#pragma once
#include <OuchOrderCommand.hpp>
#include <SPSCQueue.hpp>
#include "Order.hpp"
#include "OrderEvent.hpp"
#include <array>
#include <vector>
#include <memory>
#include <chrono>
#include <stdexcept>
#include <utility>
#include <cstring>
#include "SPSCProducerPolicy.hpp"
#include "IGoodTillDayScheduler.hpp"
#include "MarketHours.hpp"
// get_synced_time_ns() is needed unconditionally now (every OrderEvent's
// .timestamp uses it, not just an ENABLE_DETAILED_TELEMETRY build) — was
// previously guarded behind that flag here, though SPSCProducerPolicy.hpp
// already pulls Telemetry.hpp in transitively either way; made explicit
// and unconditional rather than relying on that transitive include.
#include "Telemetry.hpp"
#define WORST_ASK 100
#define WORST_BID 0

// Limit bounds to realistic limits or maintain full size via heap allocation
static constexpr std::size_t MAX_GLOBAL_ORDERS = 10'000'000u; // Sized for runtime bounds
static constexpr std::size_t MAX_LEVELS = 101u; // [0, 100] inclusive
static constexpr std::size_t BOOK_POOL_SIZE = 262'144u;

class PriceLevel {
public:
    OrderId m_head{ INVALID_IDX };
    OrderId m_tail{ INVALID_IDX };
    Quantity m_resting_quantity{ 0u };

    [[nodiscard]] Quantity get_resting_quantity() const noexcept { return m_resting_quantity; }
};

struct OrderBookBits {
    // 2 chunks of 64 bits = 128 bits total (perfectly fits 101 price levels)
    // Replaced 'unsigned __int64' with standard portable 'uint64_t'
    std::array<uint64_t, 2> m_chunks{ 0, 0 };

    // Sets a price level (0 to 100)
    void set(Price level) {
        m_chunks[level / 64] |= (1ULL << (level % 64));
    }

    void reset(Price level) {
        m_chunks[level / 64] &= ~(1ULL << (level % 64));
    }

    // Hardware-accelerated: Find the first active price level
    Price find_first_level() const {
        // Step 1: Scan the first 64 price levels (0-63)
        if (m_chunks[0] != 0) {
            // __builtin_ctzll finds the lowest set bit index (0 to 63)
            return static_cast<Price>(__builtin_ctzll(m_chunks[0]));
        }

        // Step 2: Scan the remaining price levels (64-100)
        if (m_chunks[1] != 0) {
            auto index = __builtin_ctzll(m_chunks[1]);
            auto global_index = 64 + index;
            return static_cast<Price>((global_index < 101) ? global_index : INVALID_PRICE);
        }

        return INVALID_PRICE; // No active levels found
    }

    Price find_last_level() const {
        // Scan chunk 1 first (levels 64 - 100)
        if (m_chunks[1] != 0) {
            // __builtin_clzll counts leading zeros from the MOST significant bit down.
            // 63 minus leading zeros gives us the highest set bit index.
            auto index = 63 - __builtin_clzll(m_chunks[1]);
            auto global_index = 64 + index;

            if (global_index < 101) {
                return static_cast<Price>(global_index);
            }
        }

        // Scan chunk 0 next (levels 0 - 63)
        if (m_chunks[0] != 0) {
            auto index = 63 - __builtin_clzll(m_chunks[0]);
            return static_cast<Price>(index);
        }

        return INVALID_PRICE;
    }
};

template <SIDE BookSideType, typename TelemetryPolicy>
class BookSide {
public:
    explicit BookSide(std::vector<Order*>& globalOrderLookup, TelemetryPolicy& policy,
                       uint64_t& nextSequenceNumber, ExecutionId& nextMatchId) :
        m_global_lookup(&globalOrderLookup),
        m_order_pool(std::make_unique<Order[]>(BOOK_POOL_SIZE)),
        m_free_pool(std::make_unique<OrderId[]>(BOOK_POOL_SIZE)),
        m_policy(policy),
        m_next_sequence_number(nextSequenceNumber),
        m_next_match_id(nextMatchId)
    {
        for (size_t i = 0; i < BOOK_POOL_SIZE; ++i) {
            m_free_pool[i] = static_cast<OrderId>(i);
        }
    }

    [[nodiscard]] Quantity get_resting_quantity() const noexcept { return m_quantity; }

    [[nodiscard]] Quantity get_quantity_at_or_better(Price price) {
        //TODO: Investigate simd
        Quantity cumulativeQuantity{ 0 };
        if constexpr (BookSideType == SIDE::ASK) {
            for (Price p{ price }; p > WORST_BID; --p) {
                cumulativeQuantity += m_price_levels[p].get_resting_quantity();
            }
        }
        else {
            for (Price p{ price }; p < WORST_ASK; ++p) {
                cumulativeQuantity += m_price_levels[p].get_resting_quantity();
            }
        }
        return cumulativeQuantity;
    }

    [[nodiscard]] Price get_best_price() const noexcept {

        if constexpr (BookSideType == SIDE::ASK) {
            return m_active_prices_mask.find_first_level();
        }
        else {
            return m_active_prices_mask.find_last_level();
        }
    }

    [[nodiscard]] Price get_worst_possible_price() const noexcept {
        if constexpr (BookSideType == SIDE::ASK) {
            return WORST_ASK;
        }
        else {
            return WORST_BID;
        }
    }

    
    void match_order(Order& order) {
        // 1. Single bit scan to find starting point
        Price p = get_best_price();

        // 2. Linear step loop (Branch-predictor friendly)
        while (p <= 100 && order.get_remaining_quantity() > 0u && is_crossed(p, order.get_price())) {
            // Process level p until empty OR order filled
            if (m_price_levels[p].get_resting_quantity() > 0) {
                walk_price_level(p, m_price_levels[p], order);
            }

            // If order still has quantity, move directly to the next adjacent price level
            if (order.get_remaining_quantity() == 0u) break;

            // Advance to next candidate price without invoking bitwise instructions
            if constexpr (BookSideType == SIDE::ASK) {
                ++p;
            }
            else {
                if (p == 0) break;
                --p;
            }
        }
    }

    
    void insert_order(Order& order, OrderEventType eventType = OrderEventType::ACCEPTED) {
        Price p = order.get_price();
        auto& priceLevel = m_price_levels[p];

        if (m_free_top == 0) {
            throw std::runtime_error("Order pool capacity exceeded!");
        }

        OrderId free_slot = m_free_pool[--m_free_top];
        m_order_pool[free_slot] = order;
        Order& pooled_order = m_order_pool[free_slot];
        pooled_order.m_pool_idx = free_slot;

        if (priceLevel.m_head == INVALID_IDX) {
            priceLevel.m_head = free_slot;
            priceLevel.m_tail = free_slot;
            pooled_order.m_prev_idx = INVALID_IDX;
            pooled_order.m_next_idx = INVALID_IDX;
            m_active_prices_mask.set(p);
        }
        else {
            pooled_order.m_prev_idx = priceLevel.m_tail;
            pooled_order.m_next_idx = INVALID_IDX;
            m_order_pool[priceLevel.m_tail].m_next_idx = free_slot;
            priceLevel.m_tail = free_slot;
        }

        priceLevel.m_resting_quantity += order.get_remaining_quantity();
        m_quantity += order.get_remaining_quantity();

        (*m_global_lookup)[order.get_id()] = &pooled_order;
        m_policy.on_order_event(OrderEvent{
            .type{eventType},
            .timestamp{get_synced_time_ns()},
            .sequence_number{m_next_sequence_number++},
            .order_id{order.get_id()},
            .firm_id{order.get_firm_id()},
            .session_id{order.m_session_id},
            .side{order.get_side()},
            .price{order.get_price()},
            .quantity{order.get_remaining_quantity()},
            .leaves_quantity{order.get_remaining_quantity()},
            .match_id{0},
            .reject_reason{RejectReason::NONE}
            });
    }


    void cancel_order(OrderId order_id) {
        Order* order_ptr = (*m_global_lookup)[order_id];
        if (!order_ptr) return;

        Price p = order_ptr->get_price();
        auto& priceLevel = m_price_levels[p];
        OrderId idx = order_ptr->m_pool_idx;

        if (order_ptr->m_prev_idx != INVALID_IDX) {
            m_order_pool[order_ptr->m_prev_idx].m_next_idx = order_ptr->m_next_idx;
        }
        else {
            priceLevel.m_head = order_ptr->m_next_idx;
        }

        if (order_ptr->m_next_idx != INVALID_IDX) {
            m_order_pool[order_ptr->m_next_idx].m_prev_idx = order_ptr->m_prev_idx;
        }
        else {
            priceLevel.m_tail = order_ptr->m_prev_idx;
        }

        priceLevel.m_resting_quantity -= order_ptr->get_remaining_quantity();
        m_quantity -= order_ptr->get_remaining_quantity();

        if (priceLevel.get_resting_quantity() == 0) {
            m_active_prices_mask.reset(p);
        }
        m_policy.on_trace_complete(g_telemetry_arena[trace_index(order_ptr->m_telemetry_idx)]);
        (*m_global_lookup)[order_id] = nullptr;
        m_free_pool[m_free_top++] = idx;
        m_policy.on_order_event(OrderEvent{
            .type{OrderEventType::CANCELED},
            .timestamp{get_synced_time_ns()},
            .sequence_number{m_next_sequence_number++},
            .order_id{order_ptr->get_id()},
            .firm_id{order_ptr->get_firm_id()},
            .session_id{order_ptr->m_session_id},
            .side{order_ptr->get_side()},
            .price{order_ptr->get_price()},
            .quantity{order_ptr->get_remaining_quantity()},
            .leaves_quantity{order_ptr->get_remaining_quantity()},
            .match_id{0},
            .reject_reason{RejectReason::NONE}
            });
    }

    [[nodiscard]] Quantity dry_run_available_quantity(const Order& order, Quantity needed) const noexcept {
        Quantity accumulated{ 0 };
        Price p = get_best_price();
        while (p <= 100 && accumulated < needed && is_crossed(p, order.get_price())) {
            OrderId curr = m_price_levels[p].m_head;
            while (curr != INVALID_IDX) {
                const Order& resting = m_order_pool[curr];
                if (resting.get_firm_id() == order.get_firm_id()) {
                    return accumulated; // mirrors walk_price_level's hard stop
                }
                accumulated += resting.get_remaining_quantity();
                if (accumulated >= needed) return accumulated; // early exit
                curr = resting.m_next_idx;
            }
            if constexpr (BookSideType == SIDE::ASK) {
                ++p;
            }
            else {
                if (p == 0) break;
                --p;
            }
        }
        return accumulated;
    }

private:
    [[nodiscard]] static constexpr bool is_crossed(Price bookPrice, Price incomingPrice) noexcept {
        if constexpr (BookSideType == SIDE::ASK) {
            return incomingPrice >= bookPrice;
        }
        else {
            return incomingPrice <= bookPrice;
        }
    }

    
    Quantity walk_price_level(Price p, PriceLevel& priceLevel, Order& order) {
        Quantity takenFromBook{ 0u };
        OrderId curr = priceLevel.m_head;
        OrderTrace& trace = g_telemetry_arena[trace_index(order.m_telemetry_idx)];
        ++trace.price_levels_touched;
        while (curr != INVALID_IDX && order.get_remaining_quantity() > 0u) {
            auto& topOrder = m_order_pool[curr];
            ++trace.resting_orders_touched;
            if (topOrder.get_firm_id() == order.get_firm_id()) {
                //STP prevention, we cancel the aggressive order                
                m_policy.on_order_event(OrderEvent{
                    .type{OrderEventType::REJECTED},
                    .timestamp{get_synced_time_ns()},
                    .sequence_number{m_next_sequence_number++},
                    .order_id{order.get_id()},
                    .firm_id{order.get_firm_id()},
                    .session_id{order.m_session_id},
                    .side{order.get_side()},
                    .price{order.get_price()},
                    .quantity{order.get_remaining_quantity()},
                    .leaves_quantity{0},
                    .match_id{0},
                    .reject_reason{RejectReason::SELF_TRADING_PREVENTION}
                    });
                order.cancel();
                return takenFromBook;
            }
            auto matchedQuantity = topOrder.fill(order);

            if (matchedQuantity > 0u) {
                takenFromBook += matchedQuantity;
                m_quantity -= matchedQuantity;
                priceLevel.m_resting_quantity -= matchedQuantity;
                //create 2 trade execution events one for each participant, sharing one match_id
                ExecutionId matchId = m_next_match_id++;
                m_policy.on_order_event(create_trade_execution_event(topOrder, matchedQuantity, p, matchId));
                m_policy.on_order_event(create_trade_execution_event(order, matchedQuantity, p, matchId));
            }

            if (topOrder.get_remaining_quantity() == 0u) {

                priceLevel.m_head = topOrder.m_next_idx;
                if (priceLevel.m_head != INVALID_IDX) {
                    m_order_pool[priceLevel.m_head].m_prev_idx = INVALID_IDX;
                }
                else {
                    priceLevel.m_tail = INVALID_IDX;
                }
                m_policy.on_trace_complete(g_telemetry_arena[trace_index(topOrder.m_telemetry_idx)]);
                (*m_global_lookup)[topOrder.get_id()] = nullptr;
                m_free_pool[m_free_top++] = curr;

                curr = priceLevel.m_head;
            }
        }

        if (priceLevel.get_resting_quantity() == 0u) {
            m_active_prices_mask.reset(p);
        }
        return takenFromBook;
    }

    inline OrderEvent create_trade_execution_event(Order& topOrder, Quantity matchedQuantity, Price p, ExecutionId matchId) {
        return OrderEvent{
            .type {OrderEventType::EXECUTED},
            .timestamp {get_synced_time_ns()},
            .sequence_number{m_next_sequence_number++},
            .order_id{topOrder.get_id()},
            .firm_id{topOrder.get_firm_id()},
            .session_id{topOrder.m_session_id},
            .side{topOrder.get_side()},
            .price{p},
            .quantity{matchedQuantity},
            .leaves_quantity{topOrder.get_remaining_quantity()},
            .match_id{matchId},
            .reject_reason{RejectReason::NONE}
        };
    }

    Quantity m_quantity{ 0u };
    std::array<PriceLevel, MAX_LEVELS> m_price_levels{};
    OrderBookBits m_active_prices_mask;

    // Arena storage moved off stack onto heap via unique_ptr buffers
    std::unique_ptr<Order[]> m_order_pool;
    std::unique_ptr<OrderId[]> m_free_pool;
    OrderId m_free_top{ BOOK_POOL_SIZE };
    TelemetryPolicy& m_policy; // Reference to parent policy
    std::vector<Order*>* m_global_lookup{ nullptr };
    uint64_t& m_next_sequence_number;   // Shared with the sibling BookSide and OrderBook
    ExecutionId& m_next_match_id;       // Shared with the sibling BookSide
};

template <typename TelemetryPolicy>
class OrderBook {
    using AskBook = BookSide<SIDE::ASK, TelemetryPolicy>;
    using BidBook = BookSide<SIDE::BID, TelemetryPolicy>;
public:
    OrderBook(TelemetryPolicy& policy, IGoodTillDayScheduler* gtdScheduler = nullptr)
        : m_telemetry_policy(policy)
        , m_gtd_scheduler(gtdScheduler)
        , m_resting_orders(MAX_GLOBAL_ORDERS, nullptr)
        , m_bid_book(m_resting_orders, policy, m_next_sequence_number, m_next_match_id)
        , m_ask_book(m_resting_orders, policy, m_next_sequence_number, m_next_match_id)
    {
    }


    void submit_order(OuchOrderCommand& cmd) {
        // REPLACE_ORDER needs fundamentally different handling than "build
        // an Order and dispatch on its type" — it must look up the existing
        // order to inherit side/firm/type, so it gets its own path entirely.
        if (cmd.type == CommandType::REPLACE_ORDER) {
            replace_order(cmd);
            return;
        }

        // CANCEL_ORDER carries no meaningful price (OuchProtocolHandler zeroes
        // it); only ENTER prices need to be within the book's range (REPLACE's
        // own price check happens inside replace_order), since m_price_levels
        // is indexed directly by Price with no further bounds check.
        if (cmd.type != CommandType::CANCEL_ORDER && cmd.price > WORST_ASK) {
            m_telemetry_policy.on_order_event(OrderEvent{
                .type{OrderEventType::REJECTED},
                .timestamp{get_synced_time_ns()},
                .sequence_number{m_next_sequence_number++},
                .order_id{cmd.orderId},
                .firm_id{0},
                .session_id{cmd.sessionId},
                .side{cmd.buySellIndicator == 'B' ? SIDE::BID : SIDE::ASK},
                .price{cmd.price},
                .quantity{cmd.shares},
                .leaves_quantity{0},
                .match_id{0},
                .reject_reason{RejectReason::PRICE_OUT_OF_RANGE}
                });
            // No Order was ever constructed for this reject, so nothing else
            // will ever complete this trace_id — do it here directly.
            m_telemetry_policy.on_trace_complete(g_telemetry_arena[trace_index(cmd.trace_id)]);
            return;
        }

        Order local_order = create_order_from_command(cmd);
        match_order(local_order);

        // If the order didn't end up resting — filled completely, rejected
        // by self-trade prevention, or an IOC/MARKET/FOK order that never
        // rests by design — its fate is fully decided now and nothing else
        // will ever complete its trace (unlike a resting order, whose trace
        // correctly stays open until a later cancel_order/eviction call).
        // CANCEL_ORDER is excluded: it already fully self-completes via
        // cancel_order's eviction of the target plus the cancel command's
        // own synthetic completion just below match_order() internally.
        if (cmd.type != CommandType::CANCEL_ORDER && !exists(local_order.get_id())) {
            m_telemetry_policy.on_trace_complete(g_telemetry_arena[trace_index(cmd.trace_id)]);
        }

        if (m_gtd_scheduler && cmd.orderType == ORDER_TYPE::GOOD_TILL_DAY && !local_order.is_filled()) {
            m_gtd_scheduler->schedule_cancel(cmd.orderToken, today_4pm_est_utc());
        }
    }


    void submit_order(Order& order) {
        match_order(order);

        // Same reasoning as the OuchOrderCommand overload above — keep the
        // two submit_order entry points consistent rather than letting this
        // one (used by the benchmark harness and direct-Order-construction
        // tests) silently diverge.
        if (order.get_type() != ORDER_TYPE::CANCEL && !exists(order.get_id())) {
            m_telemetry_policy.on_trace_complete(g_telemetry_arena[trace_index(order.m_telemetry_idx)]);
        }
    }

    Price get_best_bid() const { return m_bid_book.get_best_price(); }
    Price get_best_ask() const { return m_ask_book.get_best_price(); }
    Price get_bid_ask_spread() const {
        Price ask = get_best_ask();
        Price bid = get_best_bid();
        if (ask == INVALID_PRICE || bid == INVALID_PRICE || ask < bid) {
            return INVALID_PRICE;
        }
        return ask - bid;
    }
    Quantity get_bid_quantity() const { return m_bid_book.get_resting_quantity(); }
    Quantity get_ask_quantity() const { return m_ask_book.get_resting_quantity(); }

private:
    // Heap-allocated lookup vector initialized at startup
    TelemetryPolicy& m_telemetry_policy;
    IGoodTillDayScheduler* m_gtd_scheduler{ nullptr };
    std::vector<Order*> m_resting_orders;
    uint64_t m_next_sequence_number{ 1 };  // Shared across both book sides — one global event stream
    ExecutionId m_next_match_id{ 1 };      // Shared across both book sides — one id per trade, both legs
    BidBook m_bid_book;
    AskBook m_ask_book;
    Clock m_matching_clock;

    [[nodiscard]] Order create_order_from_command(const OuchOrderCommand& cmd) noexcept {
        // Wire message kind (Enter/Cancel/Replace) decides CANCEL; only an
        // ENTER_ORDER's own orderType field decides the matching behavior.
        ORDER_TYPE type = (cmd.type == CommandType::CANCEL_ORDER) ? ORDER_TYPE::CANCEL : cmd.orderType;
        Order order(cmd.orderId,
            cmd.firmId,
            cmd.buySellIndicator == 'B' ? SIDE::BID : SIDE::ASK,
            type,
            cmd.price,
            cmd.shares);
        order.m_telemetry_idx = cmd.trace_id;
        order.m_session_id = cmd.sessionId;
        return order;
    }

    // A replace can change price/quantity but not the economic identity of
    // an order — side/firm/type are inherited from whatever's being
    // replaced, same as real OUCH. cmd.orderId is the EXISTING order (see
    // OuchOrderCommand's comment); cmd.replacementOrderId/cmd.orderToken
    // are the new order that results if this succeeds.
    void replace_order(OuchOrderCommand& cmd) {
        Order* existing = (cmd.orderId < MAX_GLOBAL_ORDERS) ? m_resting_orders[cmd.orderId] : nullptr;

        if (existing == nullptr) {
            m_telemetry_policy.on_order_event(OrderEvent{
                .type{OrderEventType::REJECTED},
                .timestamp{get_synced_time_ns()},
                .sequence_number{m_next_sequence_number++},
                .order_id{cmd.orderId},
                .firm_id{0},
                .session_id{cmd.sessionId},
                .side{SIDE::NO_SIDE},
                .price{cmd.price},
                .quantity{cmd.shares},
                .leaves_quantity{0},
                .match_id{0},
                .reject_reason{RejectReason::UNKNOWN_ORDER}
                });
            m_telemetry_policy.on_trace_complete(g_telemetry_arena[trace_index(cmd.trace_id)]);
            return;
        }

        // Same bound ENTER_ORDER's own price check uses — m_price_levels is
        // indexed directly by Price with no further bounds check.
        if (cmd.price > WORST_ASK) {
            m_telemetry_policy.on_order_event(OrderEvent{
                .type{OrderEventType::REJECTED},
                .timestamp{get_synced_time_ns()},
                .sequence_number{m_next_sequence_number++},
                .order_id{cmd.orderId},
                .firm_id{existing->get_firm_id()},
                .session_id{cmd.sessionId},
                .side{existing->get_side()},
                .price{cmd.price},
                .quantity{cmd.shares},
                .leaves_quantity{0},
                .match_id{0},
                .reject_reason{RejectReason::PRICE_OUT_OF_RANGE}
                });
            m_telemetry_policy.on_trace_complete(g_telemetry_arena[trace_index(cmd.trace_id)]);
            return;
        }

        // Capture before cancel() — it frees the pool slot existing points
        // to, so the pointer would dangle afterward.
        SIDE side = existing->get_side();
        FirmId firm = existing->get_firm_id();
        ORDER_TYPE type = existing->get_type();

        cancel(cmd.orderId); // evicts + completes the existing order's own trace via cancel_order

        Order replacement(cmd.replacementOrderId, firm, side, type, cmd.price, cmd.shares);
        replacement.m_telemetry_idx = cmd.trace_id;
        replacement.m_session_id = cmd.sessionId;

        limit(replacement, OrderEventType::REPLACED);

        // Only LIMIT/GTC/GTD/MODIFY orders can ever be found via
        // m_resting_orders in the first place (IOC/MARKET/FOK never rest),
        // so limit() — not the full match_order() type dispatch — is always
        // the right path for whatever type was inherited above.
        if (!exists(replacement.get_id())) {
            m_telemetry_policy.on_trace_complete(g_telemetry_arena[trace_index(cmd.trace_id)]);
        }
    }

    void match_order(Order& order) {
        if (order.get_type() == ORDER_TYPE::CANCEL) {
            cancel(order.get_id());
            // The cancel command's own trace (ingress -> engine_pop -> match_done
            // for this cancel operation itself), distinct from the target order's
            // trace (already closed out inside cancel_order, if it was found).
            // Emitted unconditionally, whether or not a target was found, so
            // cancel latency is always observable.
            m_telemetry_policy.on_trace_complete(g_telemetry_arena[trace_index(order.m_telemetry_idx)]);
        }
        else if (order.get_type() == ORDER_TYPE::LIMIT) {
            limit(order);
        }
        else if (order.get_type() == ORDER_TYPE::FILL_OR_KILL) {
            fill_or_kill(order);
        }
        else if (order.get_type() == ORDER_TYPE::IMMEDIATE_OR_CANCEL) {
            immediate_or_cancel(order);
        }
        else if (order.get_type() == ORDER_TYPE::GOOD_TILL_CANCEL || order.get_type() == ORDER_TYPE::GOOD_TILL_DAY) {
            // GTD's scheduled auto-cancel is registered by submit_order() after
            // this returns; both rest exactly like a plain LIMIT order here.
            limit(order);
        }
        else if (order.get_type() == ORDER_TYPE::MODIFY) {
            cancel(order.get_id());
            limit(order);
        }
        else if (order.get_type() == ORDER_TYPE::MARKET) {
            market(order);
        }
        else {

            throw std::logic_error("Hit Unimplemented Code: Unknown order");
        }
    }
        
    void fill_or_kill(Order& order) {
        //this doesn't satisfy STP
        if (order.get_side() == SIDE::BID && m_ask_book.dry_run_available_quantity(order, order.get_initial_quantity()) < order.get_initial_quantity()) {
            m_telemetry_policy.on_order_event(OrderEvent{
                .type{OrderEventType::REJECTED},
                .timestamp{get_synced_time_ns()},
                .sequence_number{m_next_sequence_number++},
                .order_id{order.get_id()},
                .firm_id{order.get_firm_id()},
                .session_id{order.m_session_id},
                .side{order.get_side()},
                .price{order.get_price()},
                .quantity{order.get_remaining_quantity()},
                .leaves_quantity{0},
                .match_id{0},
                .reject_reason{RejectReason::INSUFFICIENT_LIQUIDITY}
                });
            return;
        }
        else if (order.get_side() == SIDE::ASK && m_bid_book.dry_run_available_quantity(order, order.get_initial_quantity()) < order.get_initial_quantity()) {
            m_telemetry_policy.on_order_event(OrderEvent{
                .type{OrderEventType::REJECTED},
                .timestamp{get_synced_time_ns()},
                .sequence_number{m_next_sequence_number++},
                .order_id{order.get_id()},
                .firm_id{order.get_firm_id()},
                .session_id{order.m_session_id},
                .side{order.get_side()},
                .price{order.get_price()},
                .quantity{order.get_remaining_quantity()},
                .leaves_quantity{0},
                .match_id{0},
                .reject_reason{RejectReason::INSUFFICIENT_LIQUIDITY}
                });
            return;
        }

        if (order.get_side() == SIDE::BID) {
            m_ask_book.match_order(order);
        }
        else if (order.get_side() == SIDE::ASK) {
            m_bid_book.match_order(order);
        }
        else {
            throw std::logic_error("Hit Unimplemented Code, unknown side in FOK");
        }
    }
        
    // eventType is what gets emitted if the order ends up resting — ACCEPTED
    // for a normal new order, REPLACED for a replacement (see replace_order)
    // so it doesn't also get a redundant ACCEPTED from insert_order.
    void limit(Order& order, OrderEventType eventType = OrderEventType::ACCEPTED) {
        if (order.get_side() == SIDE::BID && m_ask_book.get_best_price() == INVALID_PRICE) {
            m_bid_book.insert_order(order, eventType);
            return;
        }

        if (order.get_side() == SIDE::ASK && m_bid_book.get_best_price() == INVALID_PRICE) {
            m_ask_book.insert_order(order, eventType);
            return;
        }

        if (order.get_side() == SIDE::BID) {
            m_ask_book.match_order(order);
        }
        else if (order.get_side() == SIDE::ASK) {
            m_bid_book.match_order(order);
        }
        else {
            throw std::logic_error("Hit Unimplemented Code: Unknown side in limit");
        }

        if (!order.is_filled()) {
            if (order.get_side() == SIDE::BID) {
                m_bid_book.insert_order(order, eventType);
            }
            else {
                m_ask_book.insert_order(order, eventType);
            }
        }
    }
        
    void cancel(OrderId orderId) {
        if (orderId >= MAX_GLOBAL_ORDERS) return;
        auto order_ptr = m_resting_orders[orderId];
        if (order_ptr == nullptr) return;
        if (order_ptr->get_side() == SIDE::BID) {
            m_bid_book.cancel_order(orderId);
        }
        else {
            m_ask_book.cancel_order(orderId);
        }
    }

    bool exists(OrderId orderId) noexcept {
        if (orderId >= MAX_GLOBAL_ORDERS) return false;
        return m_resting_orders[orderId] != nullptr;
    }
        
    void emit_canceled_remainder(Order& order) {
        if (order.get_remaining_quantity() == 0u) return;
        m_telemetry_policy.on_order_event(OrderEvent{
            .type{OrderEventType::CANCELED},
            .timestamp{get_synced_time_ns()},
            .sequence_number{m_next_sequence_number++},
            .order_id{order.get_id()},
            .firm_id{order.get_firm_id()},
            .session_id{order.m_session_id},
            .side{order.get_side()},
            .price{order.get_price()},
            .quantity{order.get_remaining_quantity()},
            .leaves_quantity{0},
            .match_id{0},
            .reject_reason{RejectReason::NONE}
            });
    }

    void immediate_or_cancel(Order& order) {
        if (order.get_side() == SIDE::BID) {
            m_ask_book.match_order(order);
        }
        else if (order.get_side() == SIDE::ASK) {
            m_bid_book.match_order(order);
        }
        else {
            throw std::logic_error("Hit Unimplemented Code, unknown sdie in IOC");
        }
        emit_canceled_remainder(order);
    }
        
    void market(Order& order) {
        if (order.get_side() == SIDE::BID) {
            order.set_price(m_ask_book.get_worst_possible_price());
            m_ask_book.match_order(order);
        }
        else if (order.get_side() == SIDE::ASK) {
            order.set_price(m_bid_book.get_worst_possible_price());
            m_bid_book.match_order(order);
        }
        else {
            throw std::logic_error("Hit Unimplemented market Code: Side::NONE");
        }
        emit_canceled_remainder(order);
    }
};