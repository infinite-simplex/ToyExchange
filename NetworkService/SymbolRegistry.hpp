#pragma once
#include <cstdint>
#include <cstring>
#include <string_view>
#include <stdexcept>
#include <algorithm>

class SymbolRegistry {
public:
    static constexpr size_t MAX_SYMBOLS = 2048;
    static constexpr uint16_t INVALID_SLOT = 0xFFFF;

    SymbolRegistry() {
        // Initialize the fast lookup array with empty tokens
        for (size_t i = 0; i < BUCKET_COUNT; ++i) {
            m_lookupTable[i].rawBytes = 0;
            m_lookupTable[i].symbolLocateId = INVALID_SLOT;
        }
    }

    // Called ONLY at startup/configuration time (slow path)
    uint16_t registerSymbol(std::string_view symbolStr) {
        if (m_symbolCount >= MAX_SYMBOLS) {
            throw std::runtime_error("SymbolRegistry allocation limit reached");
        }

        // Reinterpret the characters into a clean 64-bit unsigned int
        SymbolKey key;
        key.rawBytes = 0;
        // Nasdaq strings are space-padded to exactly 8 bytes
        size_t bytesToCopy = std::min(symbolStr.size(), size_t(8));
        std::memcpy(&key.rawBytes, symbolStr.data(), bytesToCopy);

        // Standard space padding for short tickers (e.g. "GE      ")
        for (size_t i = bytesToCopy; i < 8; ++i) {
            reinterpret_cast<char*>(&key.rawBytes)[i] = ' ';
        }

        // Insert into flat hash-slot via linear probing
        size_t bucket = hash64(key.rawBytes) & (BUCKET_COUNT - 1);
        while (m_lookupTable[bucket].symbolLocateId != INVALID_SLOT) {
            if (m_lookupTable[bucket].rawBytes == key.rawBytes) {
                return m_lookupTable[bucket].symbolLocateId; // Already registered
            }
            bucket = (bucket + 1) & (BUCKET_COUNT - 1);
        }

        uint16_t assignedId = m_symbolCount++;
        m_lookupTable[bucket].rawBytes = key.rawBytes;
        m_lookupTable[bucket].symbolLocateId = assignedId;
        m_inverseTable[assignedId] = key.rawBytes; // Save for ITCH outbound translation

        return assignedId;
    }

    // CRITICAL HOT PATH METHOD: Converts 8-byte raw buffer to int in nanoseconds
    inline uint16_t lookup(const char* raw8ByteBuf) const noexcept {
        uint64_t rawBytes;
        std::memcpy(&rawBytes, raw8ByteBuf, 8);

        size_t bucket = hash64(rawBytes) & (BUCKET_COUNT - 1);

        // Loop unrolling friendly / branch predictor friendly linear probe
        while (m_lookupTable[bucket].symbolLocateId != INVALID_SLOT) {
            if (m_lookupTable[bucket].rawBytes == rawBytes) {
                return m_lookupTable[bucket].symbolLocateId;
            }
            bucket = (bucket + 1) & (BUCKET_COUNT - 1);
        }

        return INVALID_SLOT; // Symbol not found on this exchange
    }

    // Used by ITCH thread to convert an integer ID back to string bytes
    inline uint64_t getRawSymbolBytes(uint16_t symbolLocateId) const noexcept {
        if (symbolLocateId >= m_symbolCount) return 0;
        return m_inverseTable[symbolLocateId];
    }

private:
    // Multiplicative hash function for 64-bit integers (extremely fast, zero branches)
    static inline uint64_t hash64(uint64_t x) noexcept {
        x ^= x >> 30;
        x *= 0xbf58476d1ce4e5b9ULL;
        x ^= x >> 27;
        x *= 0x94d049bb133111ebULL;
        x ^= x >> 31;
        return x;
    }

    struct SymbolKey {
        uint64_t rawBytes;
        uint16_t symbolLocateId;
    };

    // Capacity must be power of 2 for fast logical bitwise masking instead of division modulo
    static constexpr size_t BUCKET_COUNT = 4096;

    SymbolKey m_lookupTable[BUCKET_COUNT];
    uint64_t m_inverseTable[MAX_SYMBOLS]{};
    uint16_t m_symbolCount = 0;
};
