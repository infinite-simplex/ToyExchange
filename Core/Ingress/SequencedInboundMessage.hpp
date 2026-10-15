#pragma once
#include <cstddef>
#include <cstdint>

template <typename TCommand>
struct SequencedInboundMessage {
    uint64_t  sequenceNumber;
    uint64_t  ingressTaiNs; // nanoseconds since the TAI epoch — see get_synced_time_ns() in Telemetry.hpp
    uint32_t  clientSessionId;
    TCommand  command;
};

// On the wire, a datagram carries one or more of these records back to back
// (multicast fan-out), or a small header and then records (retransmit reply).
// This is how many whole records fit in a datagram of at most maxBytes after
// headerBytes of header. It is never less than 1, so a record larger than the
// limit still goes out, alone.
template <typename TCommand>
constexpr size_t RecordsPerDatagram(size_t maxBytes, size_t headerBytes = 0) {
    const size_t usable = maxBytes > headerBytes ? maxBytes - headerBytes : 0;
    const size_t fit = usable / sizeof(SequencedInboundMessage<TCommand>);
    return fit > 0 ? fit : 1;
}
