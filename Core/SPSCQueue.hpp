#pragma once
#include <atomic>
#include <cstddef>
#include <new>

template <typename T, size_t Capacity = 1024>
class SPSCQueue {
public:
    SPSCQueue() : m_head(0), m_tail(0) {
        // Ensure Capacity is a power of 2 for fast bitwise modulo operations
        static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    }

    ~SPSCQueue() = default;

    // Delete copy/move constructors to prevent accidental duplication of the ring buffer
    SPSCQueue(const SPSCQueue&) = delete;
    SPSCQueue& operator=(const SPSCQueue&) = delete;

    // Enqueue: Used by the Network Ingress thread
    bool try_push(const T& item) {
        const size_t currentTail = m_tail.load(std::memory_order_relaxed);
        const size_t currentHead = m_head.load(std::memory_order_acquire);

        // Check if the ring buffer is full
        if ((currentTail - currentHead) == Capacity) {
            return false;
        }

        // Fast bitwise indexing instead of expensive modulo (%) operator
        m_buffer[currentTail & (Capacity - 1)] = item;
        m_tail.store(currentTail + 1, std::memory_order_release);
        return true;
    }

    // Dequeue: Used by the Orderbook thread
    bool try_pop(T& item) {
        const size_t currentHead = m_head.load(std::memory_order_relaxed);
        const size_t currentTail = m_tail.load(std::memory_order_acquire);

        // Check if the ring buffer is empty
        if (currentHead == currentTail) {
            return false;
        }

        item = m_buffer[currentHead & (Capacity - 1)];
        m_head.store(currentHead + 1, std::memory_order_release);
        return true;
    }

    bool empty() const {
        return m_head.load(std::memory_order_relaxed) == m_tail.load(std::memory_order_relaxed);
    }

    size_t size() const {
        size_t tail = m_tail.load(std::memory_order_relaxed);
        size_t head = m_head.load(std::memory_order_relaxed);
        return (tail >= head) ? (tail - head) : (Capacity - (head - tail));
    }

private:
    // Pre-allocated fixed-size ring buffer array to avoid runtime allocations (malloc/new)
    T m_buffer[Capacity];

    // CRITICAL LOW-LATENCY OPTIMIZATION: 
    // alignas(64) pads the variables to separate CPU cache lines.
    // This stops the Ingress thread and Orderbook thread from fighting over the same cache hardware.
    alignas(64) std::atomic<size_t> m_head;
    alignas(64) std::atomic<size_t> m_tail;
};