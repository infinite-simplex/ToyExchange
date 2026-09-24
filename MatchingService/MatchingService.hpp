#pragma once
#include "OrderBook.hpp"
#include "SPSCQueue.hpp"
#include "SPSCProducerPolicy.hpp"
#include <atomic>
#include <thread>

template <typename TCommand>
class MatchingService {
public:
	MatchingService(SPSCQueue<TCommand>& inboundQueue, OrderBook<SPSCProducerPolicy>& lob);
	~MatchingService() { stop(); }

	void start();
	void stop();
	void poll();

	// Ingress sequence number (== OuchOrderCommand::trace_id) of the most
	// recently applied command — read by LeaderHeartbeatClient so the
	// gateway's ReplicaArbiter can tell a caught-up replica from a lagging
	// one. Safe to call from another thread.
	uint64_t lastAppliedSeq() const { return m_lastAppliedSeq.load(std::memory_order_relaxed); }

private:
	OrderBook<SPSCProducerPolicy>& m_lob;
	SPSCQueue<TCommand>& m_inboundQueue;
	std::atomic<bool> m_running;
	std::thread m_matchingThread;
	std::atomic<uint64_t> m_lastAppliedSeq{ 0 };
};

#include "MatchingService.inl"
