#pragma once
#include <cstdint>

#pragma pack(push, 1)
struct ITCHAddOrder {
    char     messageType;       // Always 'A' for Add Order
    uint16_t stockLocate;       // Internal numeric ID for the stock (for fast parsing)
    uint16_t trackingNumber;    // Internal tracking data
    uint64_t timestamp;         // Nanoseconds since midnight
    uint64_t orderReferenceNumber;// Matches the ID sent in OUCH Accepted
    char     buySellIndicator;  // 'B' or 'S'
    uint32_t shares;            // Publicly visible shares
    char     stock[8];          // Ticker symbol
    uint32_t price;             // Publicly visible price
};
#pragma pack(pop)

#pragma pack(push, 1)
struct ITCHOrderExecuted {
    char     messageType;       // Always 'E' for Executed
    uint16_t stockLocate;       // Internal numeric ID for the stock
    uint16_t trackingNumber;    // Internal tracking data
    uint64_t timestamp;         // Nanoseconds since midnight
    uint64_t orderReferenceNumber;// Connects execution to the original Add Order ID
    uint32_t executedShares;    // Number of shares traded in this specific match
    uint64_t matchNumber;       // Unique execution ID for this specific trade
};
#pragma pack(pop)

