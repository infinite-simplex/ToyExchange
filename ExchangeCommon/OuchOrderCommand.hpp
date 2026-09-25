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
    FirmId      firmId;             // ENTER_ORDER only: client-supplied, drives self-trade prevention.
                                     // CANCEL/REPLACE leave this at 0 (unused; the target order's own
                                     // firm_id, stored on the resting Order, is what matters for those).
    OrderId     orderId;            // ENTER_ORDER: freshly assigned by OuchProtocolHandler.
                                     // CANCEL_ORDER/REPLACE_ORDER: resolved from the EXISTING order's
                                     // token via OrderTokenRegistry (INVALID_ORDER_ID if unknown).
    OrderId     replacementOrderId; // REPLACE_ORDER only: freshly assigned by OuchProtocolHandler for
                                     // the order that results if the replace succeeds — the same,
                                     // single id-minting counter ENTER_ORDER uses. Unused otherwise.
    Price       price;              // Pre-scaled integer (no decimals, fast comparison)
    Quantity    shares;             // Flat integer
    uint32_t    stockLocate;        // CRITICAL: String "AAPL    " is mapped to an integer (e.g., 42)
    char        orderToken[14];     // ENTER_ORDER/REPLACE_ORDER: token identifying the RESULTING order
                                     // (for REPLACE_ORDER, this is the new token, not the one being
                                     // replaced). CANCEL_ORDER: token of the order being canceled.
    char        buySellIndicator;   // 'B' or 'S'
    CommandType type;               // wire message kind: Enter/Cancel/Replace
    ORDER_TYPE  orderType{ ORDER_TYPE::LIMIT }; // matching behavior, meaningful only when type == ENTER_ORDER
};
