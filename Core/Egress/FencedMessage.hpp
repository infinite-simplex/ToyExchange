#pragma once
#include "Alias.hpp"

// Wire wrapper EgressPublisher sends and EgressGateway receives instead of
// a raw T. Lets the sink (EgressGateway) drop a message tagged with a
// stale epoch — from a replica that has since been failed over away from —
// without needing to trust the sender to have noticed it lost leadership.
// See ReplicaArbiter.hpp for how the epoch is chosen.
template <typename T>
struct FencedMessage {
    FencingEpoch epoch;
    ReplicaId replicaId;
    T payload;
};
