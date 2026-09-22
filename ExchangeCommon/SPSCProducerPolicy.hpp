#pragma once
#include "SPSCQueue.hpp"
#include "OrderEvent.hpp"
#include "Telemetry.hpp"

struct SPSCProducerPolicy {
    SPSCQueue<OrderEvent, 16384>& m_event_q;
    SPSCQueue<OrderTrace, 16384>& m_trace_q;
    std::uint64_t m_dropped_messages{ 0 };
    inline void on_trace_complete(const OrderTrace& trace) noexcept {
        if(!m_trace_q.try_push(trace)) {
            //trace q is oversubscribed, drop and keep the matching engine going
            m_dropped_messages++;
        }
    }

    inline void on_order_event(const OrderEvent& evt) noexcept {
        //these cannot be dropped
        while (!m_event_q.try_push(evt)) {}
    }
};

struct TestingPolicy {
    std::vector<OrderEvent>& m_events;
    std::vector<OrderTrace>& m_traces;

    void on_order_event(const OrderEvent& evt) {
        m_events.push_back(evt);
    }

    void on_trace_complete(const OrderTrace& trace) {
        m_traces.push_back(trace);
    }
};