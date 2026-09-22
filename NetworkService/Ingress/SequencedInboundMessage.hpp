#pragma once
#include <cstdint>

template <typename TCommand>
struct SequencedInboundMessage {
    uint64_t  sequenceNumber;
    uint64_t  ingressTsc;
    uint32_t  clientSessionId;
    TCommand  command;
};