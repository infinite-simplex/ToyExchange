#pragma once
#include "Alias.hpp"
#include "Telemetry.hpp"

// Unicast, MatchingService replica -> PerformanceServiceApp. One datagram
// per completed OrderTrace (see MatchingService/TelemetryForwarder.hpp and
// SPSCProducerPolicy::on_trace_complete). Wraps the raw OrderTrace with the
// sending replica's identity, since PerformanceService is the one place that
// needs to tell replicas apart — OrderTrace itself has no notion of which
// replica produced it.
//
// Deliberately not FencedMessage<OrderTrace>: FencedMessage's epoch field
// exists so EgressGateway can drop a stale leader's traffic, which would be
// wrong here — a standby replica's telemetry isn't stale data to discard,
// it's exactly what PerformanceService wants to see from every replica,
// leader or not.
struct TelemetryReport {
    ReplicaId replicaId;
    OrderTrace trace;
};
