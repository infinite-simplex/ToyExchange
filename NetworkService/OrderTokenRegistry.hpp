#pragma once
#include <cstdint>
#include <cstring>
#include <memory>
#include "Alias.hpp"

// Network-layer orderToken -> OrderId resolution, structurally identical to
// SymbolRegistry (fixed bucket array, linear probing, multiplicative hash) —
// just keyed on the wider 14-byte orderToken instead of an 8-byte ticker, and
// (unlike SymbolRegistry's much smaller table) heap-allocated rather than an
// inline member array: at ~2^21 slots this is ~50MB, too big to safely live
// on the stack wherever this type gets declared as a local/member (same class
// of bug as BookSide's order pools, which moved off the stack for the same
// reason — see the comment above BookSide's m_order_pool). One allocation at
// construction; insert()/resolve() themselves stay allocation-free. Owned by
// OuchProtocolHandler, touched only from the single ingress thread (same
// thread that already does symbol resolution), so no synchronization is
// needed.
//
// Append-only by design: nothing ever removes an entry. That avoids a second
// writer thread ever touching this table (the matching thread never sees it),
// at the cost of a bounded lifetime capacity — sized here for ~1M ENTER_ORDERs
// (same daily-volume assumption as TELEMETRY_POOL_SIZE). A stale entry left
// behind by an order that later filled or was canceled just resolves to an id
// that safely no-ops downstream (OrderBook::cancel already tolerates unknown
// ids), exactly like SymbolRegistry already tolerates unknown lookups.
class OrderTokenRegistry {
public:
    OrderTokenRegistry() : m_table(std::make_unique<Slot[]>(BUCKET_COUNT)) {}

    // Registers (or, on a reused token, re-points) orderToken -> id.
    // Called only for ENTER_ORDER frames.
    void insert(const char orderToken[14], OrderId id) noexcept {
        TokenKey key = make_key(orderToken);
        size_t bucket = hash(key) & (BUCKET_COUNT - 1);

        for (size_t probes = 0; probes < BUCKET_COUNT; ++probes) {
            Slot& slot = m_table[bucket];
            if (slot.id == INVALID_ORDER_ID || (slot.key.hi == key.hi && slot.key.lo == key.lo)) {
                slot.key = key;
                slot.id = id;
                return;
            }
            bucket = (bucket + 1) & (BUCKET_COUNT - 1);
        }
        // Table is completely full — drop the registration. Degraded (that
        // order becomes uncancelable by token), not catastrophic, and keeps
        // this a bounded, allocation-free, single-pass operation.
    }

    // CRITICAL HOT PATH METHOD: resolves orderToken -> OrderId, or
    // INVALID_ORDER_ID if never registered (or the table is full).
    [[nodiscard]] OrderId resolve(const char orderToken[14]) const noexcept {
        TokenKey key = make_key(orderToken);
        size_t bucket = hash(key) & (BUCKET_COUNT - 1);

        for (size_t probes = 0; probes < BUCKET_COUNT; ++probes) {
            const Slot& slot = m_table[bucket];
            if (slot.id == INVALID_ORDER_ID) {
                return INVALID_ORDER_ID; // empty slot — not registered
            }
            if (slot.key.hi == key.hi && slot.key.lo == key.lo) {
                return slot.id;
            }
            bucket = (bucket + 1) & (BUCKET_COUNT - 1);
        }
        return INVALID_ORDER_ID;
    }

private:
    struct TokenKey {
        uint64_t hi; // first 8 bytes of the 14-byte token
        uint64_t lo; // last 6 bytes, zero-extended into 8
    };

    struct Slot {
        TokenKey key{ 0, 0 };
        OrderId id{ INVALID_ORDER_ID }; // INVALID_ORDER_ID (0) marks an empty slot
    };

    static inline TokenKey make_key(const char orderToken[14]) noexcept {
        TokenKey key{ 0, 0 };
        std::memcpy(&key.hi, orderToken, 8);
        std::memcpy(&key.lo, orderToken + 8, 6);
        return key;
    }

    // Same multiplicative mixing SymbolRegistry uses, applied to both halves.
    static inline uint64_t hash64(uint64_t x) noexcept {
        x ^= x >> 30;
        x *= 0xbf58476d1ce4e5b9ULL;
        x ^= x >> 27;
        x *= 0x94d049bb133111ebULL;
        x ^= x >> 31;
        return x;
    }

    static inline uint64_t hash(const TokenKey& key) noexcept {
        return hash64(key.hi) ^ hash64(key.lo);
    }

    // ~1M entries/day expected, 2x headroom for load factor (matches
    // SymbolRegistry's MAX_SYMBOLS-to-BUCKET_COUNT ratio). Power of 2 for
    // cheap bitwise masking instead of modulo.
    static constexpr size_t BUCKET_COUNT = 2'097'152; // 2^21

    std::unique_ptr<Slot[]> m_table;
};
