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

// Unicast, gateway -> replica. Starts every reply datagram.
//
// An OK datagram holds `count` (at least 1) SequencedInboundMessage records
// back to back after this header, for sequence numbers firstSequence to
// firstSequence + count - 1. The server fills each datagram up to its size
// limit, so one request is answered with a few datagrams, not one per record.
//
// TOO_OLD and NOT_YET_ARRIVED stand alone: count is 0 and firstSequence is
// the first sequence number the server could not give. The server sends
// either one last, after any OK datagrams for the records before it.
struct RetransmitReplyHeader {
    RetransmitStatus status;
    uint8_t  _pad[3];
    uint32_t count;
    uint64_t firstSequence;
};
static_assert(sizeof(RetransmitReplyHeader) == 16, "reply header is part of the wire format");
