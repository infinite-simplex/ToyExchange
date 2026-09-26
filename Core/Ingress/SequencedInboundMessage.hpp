#pragma once
#include <cstdint>

template <typename TCommand>
struct SequencedInboundMessage {
    uint64_t  sequenceNumber;
    uint64_t  ingressTaiNs; // nanoseconds since the TAI epoch — see get_synced_time_ns() in Telemetry.hpp
    uint32_t  clientSessionId;
    TCommand  command;
};