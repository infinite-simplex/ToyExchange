#include "OuchProtocolHandler.hpp"


// ---- Wire-format helpers -------------------------------------------------
// ASSUMPTION: all multi-byte integers on the wire are big-endian.

namespace {

    uint16_t readBE16(const char* p) {
        return (static_cast<uint8_t>(p[0]) << 8) | static_cast<uint8_t>(p[1]);
    }

    uint32_t readBE32(const char* p) {
        return (static_cast<uint32_t>(static_cast<uint8_t>(p[0])) << 24)
            | (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 16)
            | (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 8)
            | static_cast<uint32_t>(static_cast<uint8_t>(p[3]));
    }

} // namespace

// ---------------------------------------------------------------------------
// Assumed frame layout:
//   [2 bytes, BE] payloadLen  -- length of everything after this field
//   [1 byte]      msgType     -- 'O' = Enter, 'X' = Cancel, 'U' = Replace
//   [payloadLen-1 bytes]      -- type-specific body, see each case below
//
// NOTE on error policy: a malformed frame (bad length, unknown type, unknown
// symbol) currently returns 0, which stalls that connection's buffer rather
// than disconnecting it or skipping the bad frame. This is a placeholder —
// decide on a real policy (disconnect / reject message / skip-and-resync)
// before this touches real traffic. See flagged spots below.
// ---------------------------------------------------------------------------

size_t OuchProtocolHandler::validateAndParse(const char* buf, size_t availableLen, OuchOrderCommand& outCmd) {
    constexpr size_t LENGTH_FIELD_SIZE = 2;
    constexpr size_t TYPE_FIELD_SIZE = 1;
    constexpr size_t HEADER_SIZE = LENGTH_FIELD_SIZE + TYPE_FIELD_SIZE;

    if (availableLen < LENGTH_FIELD_SIZE) {
        return 0; // not even enough to read the length field yet
    }

    uint16_t payloadLen = readBE16(buf); // length of (type byte + body)
    size_t totalFrameLen = LENGTH_FIELD_SIZE + payloadLen;

    if (availableLen < totalFrameLen) {
        return 0; // full frame hasn't arrived yet
    }
    if (payloadLen < TYPE_FIELD_SIZE) {
        return 0; // FLAG: malformed frame — see NOTE above on error policy
    }

    char msgType = buf[LENGTH_FIELD_SIZE];
    const char* body = buf + HEADER_SIZE;
    size_t bodyLen = payloadLen - TYPE_FIELD_SIZE;

    switch (msgType) {
    case 'O': { // ENTER_ORDER
        constexpr size_t EXPECTED_BODY_LEN = 14 + 1 + 1 + 4 + 8 + 4 + 4;
        if (bodyLen != EXPECTED_BODY_LEN) {
            return 0; // FLAG: malformed — see NOTE above
        }

        size_t off = 0;

        std::memcpy(outCmd.orderToken, body + off, 14);
        off += 14;

        outCmd.buySellIndicator = body[off];
        off += 1;

        constexpr uint8_t MAX_VALID_ORDER_TYPE = static_cast<uint8_t>(ORDER_TYPE::MARKET);
        uint8_t orderTypeByte = static_cast<uint8_t>(body[off]);
        off += 1;
        if (orderTypeByte > MAX_VALID_ORDER_TYPE) {
            return 0; // FLAG: unknown order type — see NOTE above on error policy
        }
        outCmd.orderType = static_cast<ORDER_TYPE>(orderTypeByte);

        outCmd.shares = readBE32(body + off);
        off += 4;

        const char* symbol = body + off;
        off += 8;

        uint16_t stockLocate = m_symbolRegistry.lookup(symbol);
        if (stockLocate == SymbolRegistry::INVALID_SLOT) {
            return 0; // FLAG: unknown symbol — see NOTE above on error policy
        }
        outCmd.stockLocate = stockLocate; // widens uint16_t -> uint32_t, fine

        outCmd.price = readBE32(body + off);
        off += 4;

        outCmd.firmId = readBE32(body + off);
        off += 4;

        outCmd.orderId = m_nextOrderId++;
        m_orderTokenRegistry.insert(outCmd.orderToken, outCmd.orderId);

        outCmd.type = CommandType::ENTER_ORDER;
        return totalFrameLen;
    }

    case 'X': { // CANCEL_ORDER
        constexpr size_t EXPECTED_BODY_LEN = 14 + 4;
        if (bodyLen != EXPECTED_BODY_LEN) {
            return 0; // FLAG: malformed — see NOTE above
        }

        size_t off = 0;

        std::memcpy(outCmd.orderToken, body + off, 14);
        off += 14;

        outCmd.shares = readBE32(body + off);
        off += 4;

        // Not found (INVALID_ORDER_ID) is accepted, not rejected: a cancel
        // racing a fill/earlier cancel is routine, not a protocol error.
        // OrderBook's cancel path already no-ops safely on an unknown id.
        outCmd.orderId = m_orderTokenRegistry.resolve(outCmd.orderToken);

        outCmd.price = 0;
        outCmd.stockLocate = 0;
        outCmd.buySellIndicator = '\0';
        outCmd.firmId = 0;
        outCmd.type = CommandType::CANCEL_ORDER;
        return totalFrameLen;
    }

    case 'U': { // REPLACE_ORDER
        // Two tokens, matching real OUCH: the existing order's token (to
        // find what's being replaced) and a new token (identifying the
        // resulting order going forward) — not the same token reused,
        // since a token is meant to identify one specific order instance.
        constexpr size_t EXPECTED_BODY_LEN = 14 + 14 + 4 + 4;
        if (bodyLen != EXPECTED_BODY_LEN) {
            return 0; // FLAG: malformed — see NOTE above
        }

        size_t off = 0;

        char existingOrderToken[14];
        std::memcpy(existingOrderToken, body + off, 14);
        off += 14;

        std::memcpy(outCmd.orderToken, body + off, 14); // the NEW token
        off += 14;

        outCmd.shares = readBE32(body + off);
        off += 4;

        outCmd.price = readBE32(body + off);
        off += 4;

        // Not found (INVALID_ORDER_ID) is accepted, not rejected here — the
        // same reasoning as CANCEL_ORDER: OrderBook rejects a replace that
        // references an unknown order, this layer just resolves the token.
        outCmd.orderId = m_orderTokenRegistry.resolve(existingOrderToken);

        // Eagerly minted and registered, same as ENTER_ORDER, even though
        // the replace might still be rejected by OrderBook (e.g. the
        // existing order wasn't found) — harmless either way, since an id
        // that was never actually inserted just safely no-ops on any later
        // lookup, same tolerant pattern OrderTokenRegistry already documents.
        outCmd.replacementOrderId = m_nextOrderId++;
        m_orderTokenRegistry.insert(outCmd.orderToken, outCmd.replacementOrderId);

        outCmd.stockLocate = 0;
        outCmd.buySellIndicator = '\0';
        outCmd.firmId = 0;
        outCmd.type = CommandType::REPLACE_ORDER;
        return totalFrameLen;
    }

    default:
        return 0; // FLAG: unknown type byte — see NOTE above
    }
}