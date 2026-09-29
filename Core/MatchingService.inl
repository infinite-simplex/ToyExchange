#include "Telemetry.hpp"

template <typename TCommand, typename TEngine>
MatchingService<TCommand, TEngine>::MatchingService(SPSCQueue<TCommand>& inboundQueue, TEngine& engine)
	: m_engine{ engine }
	, m_inboundQueue{ inboundQueue }
	{ }

template <typename TCommand, typename TEngine>
void MatchingService<TCommand, TEngine>::start() {
	if (m_running.load()) return;
	m_running.store(true);

	m_matchingThread = std::thread(&MatchingService::poll, this);
}

template <typename TCommand, typename TEngine>
void MatchingService<TCommand, TEngine>::stop() {
	if (!m_running.load()) return;
	m_running.store(false);
	if (m_matchingThread.joinable()) m_matchingThread.join();
}

template <typename TCommand, typename TEngine>
void MatchingService<TCommand, TEngine>::poll() {
	while (m_running.load(std::memory_order_relaxed)) {
		TCommand cmd;

		bool popped = false;
		while (!(popped = m_inboundQueue.try_pop(cmd))) {
			if (!m_running.load(std::memory_order_relaxed)) break;
			//try try try again, gotta keep that latency low
		}
		if (!popped) break; // shutting down while waiting for the next command

		OrderTrace& trace = g_telemetry_arena[trace_index(cmd.trace_id)];
		trace.engine_pop_tai_ns = get_synced_time_ns(); // compared against ingress_tai_ns, stamped on a different machine
		m_engine.submit_order(cmd);
		trace.match_done_tai_ns = get_synced_time_ns();
		m_lastAppliedSeq.store(cmd.trace_id, std::memory_order_relaxed);
	}
}
