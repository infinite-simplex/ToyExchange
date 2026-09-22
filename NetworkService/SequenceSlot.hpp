#pragma once
#include <atomic>
#include "SequencedInboundMessage.hpp"

// version: even = stable/readable, odd = write-in-progress.
// Reader retries if version is odd, or changes across the copy.
template <typename TCommand>
struct alignas(64) SequenceSlot {
    std::atomic<uint64_t> version{ 0 };
    SequencedInboundMessage<TCommand> record;
};