#pragma once
#include <cstdint>
#include "Alias.hpp"
#include "OrderTypes.hpp"

enum class CommandType : uint8_t {
    ENTER_ORDER,
    CANCEL_ORDER,
    REPLACE_ORDER
};


// This is the templated type for: SPSCQueue<InboundOrderCommand>
struct OuchOrderCommand {
    TraceId     trace_id;           // Token to track this specific execution
    SessionId   sessionId;          // Owning connection, stamped by NetworkGateway at parse time — see Alias.hpp
    OrderId     orderId;            // ENTER_ORDER: freshly assigned by OuchProtocolHandler.
                                     // CANCEL_ORDER: resolved from orderToken via OrderTokenRegistry
                                     // (INVALID_ORDER_ID if the token isn't known).
    Price       price;              // Pre-scaled integer (no decimals, fast comparison)
    Quantity    shares;             // Flat integer
    uint32_t    stockLocate;        // CRITICAL: String "AAPL    " is mapped to an integer (e.g., 42)
    char        orderToken[14];     // Preserved to map back to client session
    char        buySellIndicator;   // 'B' or 'S'
    CommandType type;               // wire message kind: Enter/Cancel/Replace
    ORDER_TYPE  orderType{ ORDER_TYPE::LIMIT }; // matching behavior, meaningful only when type == ENTER_ORDER
};
