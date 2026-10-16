#pragma once
#include "Alias.hpp"

#include <cstddef>
#include <cstdint>
#include <type_traits>

// Wire format of the replicas' egress multicast: one datagram is this header, then `count`
// events of the product's event type, back to back.
//
//   epoch         The fencing epoch the sender believed it held when it sent. EgressGateway drops a
//                 datagram whose epoch is older than the arbiter's (see ReplicaArbiter.hpp).
//   firstOrdinal  Where the first event sits in the sender's event stream. A replica numbers every
//                 event it produces from 0, whether or not it publishes it, so on deterministic
//                 replicas the same ordinal names the same event. EgressGateway uses it to count
//                 events that never arrived. It is also what lets every replica publish and the
//                 gateway keep the first copy of each event.
//   replicaId     The sender.
//   count         Events that follow, at least 1.
struct EgressBatchHeader {
    FencingEpoch epoch;
    uint64_t firstOrdinal;
    ReplicaId replicaId;
    uint16_t count;
    uint32_t reserved; // zero
};
static_assert(sizeof(EgressBatchHeader) == 24, "the egress header is part of the wire format");
static_assert(std::is_trivially_copyable_v<EgressBatchHeader>);

// How many whole events fit in one egress datagram of at most maxBytes. Never less than 1, so an
// event larger than the limit still goes out, alone.
template <typename TEvent>
constexpr size_t EventsPerEgressDatagram(size_t maxBytes) {
    static_assert(std::is_trivially_copyable_v<TEvent>, "events are sent as raw bytes");
    const size_t usable = maxBytes > sizeof(EgressBatchHeader) ? maxBytes - sizeof(EgressBatchHeader) : 0;
    const size_t fit = usable / sizeof(TEvent);
    // The count field is 16 bits.
    const size_t capped = fit > UINT16_MAX ? UINT16_MAX : fit;
    return capped > 0 ? capped : 1;
}
