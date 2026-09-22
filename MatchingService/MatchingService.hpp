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

private:
	OrderBook<SPSCProducerPolicy>& m_lob;
	SPSCQueue<TCommand>& m_inboundQueue;
	std::atomic<bool> m_running;
	std::thread m_matchingThread;
};

#include "MatchingService.inl"
