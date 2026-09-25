#pragma once
#include <cstdint>
#include "Alias.hpp"
#include "OrderTypes.hpp" // SIDE
enum class OrderEventType : uint8_t {
    ACCEPTED,   // OUCH: Order Accepted        — order validated and live
    REJECTED,   // OUCH: Order Rejected        — order never entered the book
    EXECUTED,   // OUCH: Order Executed / ITCH: Order Executed — full or partial fill
    CANCELED,   // OUCH: Order Canceled / ITCH: Order Cancel — resting qty reduced
    DELETED,    // ITCH: Order Delete          — order fully removed (0 left)
    REPLACED,   // OUCH/ITCH: Order Replace    — price/qty/id changed in place
};

enum class RejectReason : uint8_t {
    NONE = 0,
    UNKNOWN_ORDER,        // e.g. cancel/replace referenced a bad id
    INVALID_QUANTITY,
    SELF_TRADING_PREVENTION,
    INSUFFICIENT_LIQUIDITY,
    PRICE_OUT_OF_RANGE,   // price outside [WORST_BID, WORST_ASK]
    UNKNOWN_SYMBOL,       // ENTER_ORDER named a symbol SymbolRegistry never registered
};

struct OrderEvent {
    OrderEventType type;
    Timestamp      timestamp;        // exchange-side event time
    uint64_t       sequence_number;  // monotonic, per-feed — lets subscribers detect gaps

    OrderId  order_id;        // the order this event is about
    FirmId   firm_id;         // 0/omitted on a public feed; populated on a private ack
    SessionId session_id;     // owning connection, for routing the private OUCH ack — see Alias.hpp
    SIDE     side;

    Price    price;           // fill price for EXECUTED; original price for ACCEPTED/CANCELED
    Quantity quantity;        // qty filled / canceled / originally accepted, depending on type
    Quantity leaves_quantity; // remaining resting size after this event (0 if DELETED)

    ExecutionId  match_id;        // links the two EXECUTED events (and a public Trade) from one match
    RejectReason reject_reason; // meaningful only when type == REJECTED
};