#pragma once
#include <cstdint>
#include "Alias.hpp"

// Replica -> gateway, UDP unicast, roughly every 2ms (see
// LeaderHeartbeatClient). lastAppliedSeq is the ingress sequence number
// (OuchOrderCommand::trace_id) the replica's matching thread has most
// recently applied — used by ReplicaArbiter to exclude a replica that is
// alive but too far behind to safely lead.
struct HeartbeatRequest {
    ReplicaId replicaId;
    uint64_t lastAppliedSeq;
};

// Gateway -> replica, same UDP round trip. The current fencing epoch and
// which replica (if any) currently holds it.
struct HeartbeatResponse {
    FencingEpoch epoch;
    ReplicaId leaderId;
};
