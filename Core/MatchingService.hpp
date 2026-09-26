#pragma once
#include "SPSCQueue.hpp"
#include <atomic>
#include <cstdint>
#include <thread>

// Generic replica host: pops TCommand off inboundQueue and hands each one to
// TEngine::submit_order(cmd) — continuous matching (OrderBook) applies it
// immediately; a batch-clearing engine could stage it and only act on a
// dedicated boundary command. This class knows nothing about either — it
// only requires TCommand to expose a .trace_id and TEngine to expose
// submit_order(TCommand&).
template <typename TCommand, typename TEngine>
class MatchingService {
public:
	MatchingService(SPSCQueue<TCommand>& inboundQueue, TEngine& engine);
	~MatchingService() { stop(); }

	void start();
	void stop();
	void poll();

	// Ingress sequence number (== TCommand::trace_id) of the most recently
	// applied command — read by LeaderHeartbeatClient so the gateway's
	// ReplicaArbiter can tell a caught-up replica from a lagging one. Safe
	// to call from another thread.
	uint64_t lastAppliedSeq() const { return m_lastAppliedSeq.load(std::memory_order_relaxed); }

private:
	TEngine& m_engine;
	SPSCQueue<TCommand>& m_inboundQueue;
	std::atomic<bool> m_running;
	std::thread m_matchingThread;
	std::atomic<uint64_t> m_lastAppliedSeq{ 0 };
};

#include "MatchingService.inl"
