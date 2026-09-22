#pragma once
#include <cstdint> 
#include "Alias.hpp"


struct GpuMatchingCommand {
    uint64_t    clientSessionId;
    TraceId     trace_id;
    uint32_t    region;
    uint32_t    hours;
    // ...whatever else this protocol actually carries
};