#pragma once
#include "SPSCQueue.hpp"
#include "SequencedInboundMessage.hpp"
#include "Telemetry.hpp"

#include <atomic>
#include <cstdint>
#include <thread>

// The one place a sequenced record becomes a command in a replica's inbound
// queue, whichever transport delivered it (UDP multicast, a gap-fill reply, or
// anything else a receiver is built on).
//
// This is the "Ingress Server Thread" from OrderTrace's point of view for this
// process: stamps the ingress_tai_ns NetworkGateway captured at socket read
// (carried over the wire in the message, as a synced wall-clock reading since
// it's read back on a different machine — see get_synced_time_ns() in
// Telemetry.hpp), times the enqueue itself with cheap local rdtsc
// (self-consistent, both reads on this machine), and records the resulting
// queue depth — then pushes into the local Orderbook Input Disruptor (SPSC
// Queue). Recovered records get the same telemetry as ones that arrived live.
//
// Returns false only if `running` went false mid-push (the queue stayed full).
template <typename TCommand, size_t Capacity>
bool PushToInboundQueue(SPSCQueue<TCommand, Capacity>& queue,
    const SequencedInboundMessage<TCommand>& msg, const std::atomic<bool>& running) {
    OrderTrace& trace = g_telemetry_arena[trace_index(msg.command.trace_id)];

    // This is the sole slot-claim site: if the slot's previous occupant
    // never got a chance to release it (complete_trace() below) before
    // wrapping all the way back around to us, that's a genuine
    // wraparound collision — most likely a long-resting GTC order whose
    // telemetry is about to be silently overwritten. Not preventable
    // without unbounded memory; at least make it observable.
    if (trace.owner_trace_id != INVALID_TRACE_ID && trace.owner_trace_id != msg.command.trace_id) {
        g_telemetry_arena_collisions.fetch_add(1, std::memory_order_relaxed);
    }
    trace.owner_trace_id = msg.command.trace_id;

    trace.ingress_tai_ns = msg.ingressTaiNs;
    trace.replica_ingress_tai_ns = get_synced_time_ns();

    uint64_t start = get_time();
    while (!queue.try_push(msg.command)) {
        if (!running.load(std::memory_order_relaxed)) return false;
        std::this_thread::yield();
    }
    trace.inbound_q_submitted = get_time() - start;
    trace.inbound_q_depth = static_cast<uint32_t>(queue.size());
    return true;
}
