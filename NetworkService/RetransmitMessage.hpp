#pragma once
#include <cstdint>

enum class RetransmitStatus : uint8_t {
    OK = 0,
    TOO_OLD = 1,          // requested seq has been evicted from the ring
    NOT_YET_ARRIVED = 2,  // requested seq hasn't been sequenced yet
};

// Unicast, replica -> gateway.
struct RetransmitRequest {
    uint64_t startSeq;
    uint32_t count; // server caps this — see MAX_BATCH
};

// Unicast, gateway -> replica. Precedes each record in an OK reply;
// stands alone (no trailing record) for TOO_OLD / NOT_YET_ARRIVED.
struct RetransmitRecordHeader {
    RetransmitStatus status;
    uint8_t  _pad[7];
    uint64_t sequenceNumber;
};