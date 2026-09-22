#include "Telemetry.hpp"

template <typename TCommand>
MatchingService<TCommand>::MatchingService(SPSCQueue<TCommand>& inboundQueue, OrderBook<SPSCProducerPolicy>& lob)
	: m_inboundQueue{ inboundQueue }
	, m_lob{ lob }
	{ }

template <typename TCommand>
void MatchingService<TCommand>::start() {
	if (m_running.load()) return;
	m_running.store(true);

	m_matchingThread = std::thread(&MatchingService::poll, this);
}

template <typename TCommand>
void MatchingService<TCommand>::stop() {
	if (!m_running.load()) return;
	m_running.store(false);
	if (m_matchingThread.joinable()) m_matchingThread.join();
}

template <typename TCommand>
void MatchingService<TCommand>::poll() {
	while (m_running.load(std::memory_order_relaxed)) {
		TCommand cmd;

		bool popped = false;
		while (!(popped = m_inboundQueue.try_pop(cmd))) {
			if (!m_running.load(std::memory_order_relaxed)) break;
			//try try try again, gotta keep that latency low
		}
		if (!popped) break; // shutting down while waiting for the next command

		OrderTrace& trace = g_telemetry_arena[trace_index(cmd.trace_id)];
		trace.engine_pop_tsc = get_time();
		//we have an inbound command, send to the orderbook
		m_lob.submit_order(cmd);
		trace.match_done_tsc = get_time();
	}
}
