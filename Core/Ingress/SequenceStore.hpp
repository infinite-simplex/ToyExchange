#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <stdexcept>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include "SequenceSlot.hpp"

template <typename TCommand>
class SequenceStore {
public:
    SequenceStore(const std::string& path, size_t capacitySlots)
        : m_capacity(capacitySlots) {

        size_t fileSize = sizeof(SequenceSlot<TCommand>) * capacitySlots;

        m_fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
        if (m_fd < 0) {
            throw std::system_error(errno, std::generic_category(), "Unable to open sequence store file");
        }
        if (ftruncate(m_fd, fileSize) < 0) {
            close(m_fd);
            throw std::system_error(errno, std::generic_category(), "Unable to size sequence store file");
        }

        void* mapped = mmap(nullptr, fileSize, PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE, m_fd, 0);
        if (mapped == MAP_FAILED) {
            close(m_fd);
            throw std::system_error(errno, std::generic_category(), "Unable to mmap sequence store");
        }

        m_slots = static_cast<SequenceSlot<TCommand>*>(mapped);
        m_fileSize = fileSize;

        // MAP_POPULATE should prefault, but zero explicitly too, so a
        // pre-existing file's leftover bytes never masquerade as a valid slot.
        // Raw file-backed bytes, not constructed objects, so the void* cast is
        // deliberate (silences -Wclass-memaccess for non-trivial TCommand).
        std::memset(static_cast<void*>(m_slots), 0, fileSize);
        mlock(m_slots, fileSize); // best-effort; fine if it's denied
    }

    ~SequenceStore() {
        if (m_slots) munmap(m_slots, m_fileSize);
        if (m_fd != -1) close(m_fd);
    }

    // Single-writer only — call from the ingress thread, same invariant as
    // m_sequenceNumber. Not safe for concurrent writers.
    void append(const SequencedInboundMessage<TCommand>& msg) {
        SequenceSlot<TCommand>& slot = m_slots[msg.sequenceNumber % m_capacity];

        uint64_t v = slot.version.load(std::memory_order_relaxed);
        slot.version.store(v + 1, std::memory_order_release); // odd: writing
        std::memcpy(&slot.record, &msg, sizeof(msg));
        slot.version.store(v + 2, std::memory_order_release); // even: stable

        m_highestPublishedSeq.store(static_cast<int64_t>(msg.sequenceNumber),
            std::memory_order_release);
    }

    enum class ReadResult { OK, TOO_OLD, NOT_YET_ARRIVED };

    // Safe to call concurrently with append(), from the retransmit thread.
    ReadResult read(uint64_t seq, SequencedInboundMessage<TCommand>& out) const {
        int64_t highest = m_highestPublishedSeq.load(std::memory_order_acquire);
        if (highest < 0 || seq > static_cast<uint64_t>(highest)) {
            return ReadResult::NOT_YET_ARRIVED;
        }

        const SequenceSlot<TCommand>& slot = m_slots[seq % m_capacity];

        for (int attempt = 0; attempt < 4; ++attempt) {
            uint64_t v1 = slot.version.load(std::memory_order_acquire);
            if (v1 & 1) continue; // writer mid-update

            std::memcpy(&out, &slot.record, sizeof(out));

            uint64_t v2 = slot.version.load(std::memory_order_acquire);
            if (v1 == v2) {
                // Stable read, but confirm it's actually the slot we wanted —
                // if the ring wrapped since we checked `highest`, this slot
                // may already hold a newer message.
                return (out.sequenceNumber == seq) ? ReadResult::OK : ReadResult::TOO_OLD;
            }
        }
        return ReadResult::TOO_OLD; // contended past retry budget; treat as evicted
    }

private:
    int m_fd = -1;
    SequenceSlot<TCommand>* m_slots = nullptr;
    size_t m_fileSize = 0;
    size_t m_capacity;
    std::atomic<int64_t> m_highestPublishedSeq{ -1 }; // -1: nothing published yet
};