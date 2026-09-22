#pragma once
#include <cstdint>

// NASDAQ O*U*C*H Version 4.2 (revision Oct 2025) — byte-accurate wire structs
// for the order-lifecycle subset this exchange actually models: entering,
// replacing, and canceling orders, and the resulting acknowledgements/
// executions. Verified field-by-field (name, offset, length) against the
// official spec: https://www.nasdaqtrader.com/content/technicalsupport/specifications/tradingproducts/ouch4.2.pdf
//
// Deliberately NOT implemented (out of scope — this exchange has no
// machinery for any of it): Modify Order, Order Priority Update, Order
// Modified, AIQ Cancelled, Executed with Reference Price, Cancel Pending,
// Cancel Reject — all either auction/cross-related or newer optional
// extensions beyond the core enter/replace/cancel/execute lifecycle.
//
// All integer fields are unsigned big-endian (network byte order) on the
// wire, same as this project's other wire formats — these structs describe
// field layout only; byte-swapping on read/write is the parser's job (see
// OuchProtocolHandler's readBE16/readBE32-style helpers).
// Timestamp fields are nanoseconds since midnight (local exchange time), and
// are 8 bytes wide in OUCH — unlike ITCH's 6-byte timestamp.

// ===========================================================================
// Inbound (participant -> OUCH host)
// ===========================================================================

#pragma pack(push, 1)
struct OUCHEnterOrder {
    char     messageType;       // 'O'
    char     orderToken[14];    // Day-unique per OUCH account; participant-assigned
    char     buySellIndicator;  // 'B' buy, 'S' sell, 'T' sell short, 'E' sell short exempt
    uint32_t shares;            // > 0 and < 1,000,000
    char     stock[8];          // Ticker symbol, space-padded
    uint32_t price;             // Fixed point, 6 whole + 4 decimal digits (e.g. $10.0000 -> 100000)
    uint32_t timeInForce;       // Seconds to live; 0 = IOC, 99998/99999/99996 = special (see spec)
    char     mpid[4];           // Firm identifier; blank = default firm for the OUCH account
    char     display;           // 'A','Y','N','P','I','M','W','L','O','T','Q','m','n','B'
    char     capacity;          // 'A' agency, 'P' principal, 'R' riskless (else -> 'O' other)
    char     intermarketSweep;  // 'Y' eligible, 'N' not eligible, 'y' trade-at ISO
    uint32_t minimumQuantity;   // Minimum acceptable execution quantity
    char     crossType;         // 'N','O','C','H','S','E','A'
    char     customerType;      // 'R' retail, 'N' not retail, ' ' = port default
};
#pragma pack(pop)
static_assert(sizeof(OUCHEnterOrder) == 49, "OUCH 4.2 Enter Order must be exactly 49 bytes");

#pragma pack(push, 1)
struct OUCHReplaceOrder {
    char     messageType;              // 'U'
    char     existingOrderToken[14];   // Token of the order being replaced
    char     replacementOrderToken[14];// New, day-unique token for the replacement
    uint32_t shares;                   // Total shares liable for the whole replace chain
    uint32_t price;
    uint32_t timeInForce;
    char     display;
    char     intermarketSweep;
    uint32_t minimumQuantity;
};
#pragma pack(pop)
static_assert(sizeof(OUCHReplaceOrder) == 47, "OUCH 4.2 Replace Order must be exactly 47 bytes");

#pragma pack(push, 1)
struct OUCHCancelOrder {
    char     messageType;     // 'X'
    char     orderToken[14];  // Token of the order to cancel/reduce
    uint32_t shares;          // New intended order size; 0 = cancel all remaining
};
#pragma pack(pop)
static_assert(sizeof(OUCHCancelOrder) == 19, "OUCH 4.2 Cancel Order must be exactly 19 bytes");

// ===========================================================================
// Outbound (OUCH host -> participant)
// ===========================================================================

#pragma pack(push, 1)
struct OUCHSystemEvent {
    char     messageType; // 'S'
    uint64_t timestamp;
    char     eventCode;   // 'S' start of day, 'E' end of day
};
#pragma pack(pop)
static_assert(sizeof(OUCHSystemEvent) == 10, "OUCH 4.2 System Event must be exactly 10 bytes");

#pragma pack(push, 1)
struct OUCHOrderAccepted {
    char     messageType;         // 'A'
    uint64_t timestamp;
    char     orderToken[14];      // As entered
    char     buySellIndicator;
    uint32_t shares;              // Accepted shares
    char     stock[8];
    uint32_t price;                    // Accepted price (may differ from entered — always at least as good)
    uint32_t timeInForce;
    char     mpid[4];
    char     display;
    uint64_t orderReferenceNumber; // Day-unique id NASDAQ assigned to this order
    char     capacity;
    char     intermarketSweep;
    uint32_t minimumQuantity;
    char     crossType;
    char     orderState;          // 'L' live, 'D' dead (accepted then auto-canceled)
    char     bboWeightIndicator;  // '0'-'3', 'S', 'N', or space
};
#pragma pack(pop)
static_assert(sizeof(OUCHOrderAccepted) == 66, "OUCH 4.2 Accepted must be exactly 66 bytes");

#pragma pack(push, 1)
struct OUCHOrderRejected {
    char     messageType;    // 'J'
    uint64_t timestamp;
    char     orderToken[14]; // As entered — cannot be reused
    char     reason;         // See spec's Rejected Order Reasons table
};
#pragma pack(pop)
static_assert(sizeof(OUCHOrderRejected) == 24, "OUCH 4.2 Rejected must be exactly 24 bytes");

#pragma pack(push, 1)
struct OUCHOrderExecuted {
    char     messageType;    // 'E'
    uint64_t timestamp;
    char     orderToken[14]; // The executing order
    uint32_t executedShares; // Incremental, not cumulative
    uint32_t executionPrice;
    char     liquidityFlag;  // See spec's Liquidity Flag Values table
    uint64_t matchNumber;    // Shared by both legs (buy + sell) of one match
};
#pragma pack(pop)
static_assert(sizeof(OUCHOrderExecuted) == 40, "OUCH 4.2 Executed must be exactly 40 bytes");

#pragma pack(push, 1)
struct OUCHOrderCanceled {
    char     messageType;      // 'C'
    uint64_t timestamp;
    char     orderToken[14];   // The order being reduced
    uint32_t decrementShares;  // Incremental, not cumulative — order may still be partly live
    char     reason;           // See spec's Cancel Order Reasons table
};
#pragma pack(pop)
static_assert(sizeof(OUCHOrderCanceled) == 28, "OUCH 4.2 Canceled must be exactly 28 bytes");

#pragma pack(push, 1)
struct OUCHOrderReplaced {
    char     messageType;              // 'U'
    uint64_t timestamp;
    char     replacementOrderToken[14];// As entered on the Replace Order Message
    char     buySellIndicator;         // As entered on the original order in the chain
    uint32_t shares;                   // Total shares outstanding after the replace
    char     stock[8];
    uint32_t price;
    uint32_t timeInForce;
    char     mpid[4];
    char     display;
    uint64_t orderReferenceNumber;     // New day-unique id for the replacement
    char     capacity;
    char     intermarketSweep;
    uint32_t minimumQuantity;
    char     crossType;
    char     orderState;               // 'L' live, 'D' dead
    char     previousOrderToken[14];   // Token of the order that was just replaced
    char     bboWeightIndicator;
};
#pragma pack(pop)
static_assert(sizeof(OUCHOrderReplaced) == 80, "OUCH 4.2 Replaced must be exactly 80 bytes");

#pragma pack(push, 1)
struct OUCHBrokenTrade {
    char     messageType;    // 'B'
    uint64_t timestamp;
    char     orderToken[14]; // Order the broken Match Number belongs to
    uint64_t matchNumber;    // As transmitted in the Executed message being broken
    char     reason;         // 'E' erroneous, 'C' consent, 'S' supervisory, 'X' external
};
#pragma pack(pop)
static_assert(sizeof(OUCHBrokenTrade) == 32, "OUCH 4.2 Broken Trade must be exactly 32 bytes");
