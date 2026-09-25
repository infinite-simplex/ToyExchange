#include "OuchProtocolHandler.hpp"
#include "OUCH.hpp"


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

    // wirePrice is OUCH's real fixed-point value (dollars * 10000, e.g.
    // $0.42 -> 4200). This exchange's Price is whole cents, 0-100
    // (WORST_BID..WORST_ASK). Since cents = dollars * 100, and the wire
    // value already is dollars * 10000, dividing by 100 recovers cents
    // exactly — an unmodified real OUCH client needs no special-casing to
    // trade here. A sub-cent remainder (wirePrice % 100 != 0) is truncated,
    // not rejected: this exchange's domain cannot represent it at all, and
    // rejecting a client for using OUCH's full fixed-point precision would
    // defeat the point of accepting real, unmodified OUCH frames.
    bool decodePrice(uint32_t wirePrice, Price& outPrice) {
        uint32_t cents = wirePrice / 100;
        if (cents > WORST_ASK) return false;
        outPrice = static_cast<Price>(cents); // narrows only after the bounds check
        return true;
    }

    // Real OUCH's ENTER_ORDER has no order-type byte — this exchange's own
    // ORDER_TYPE distinctions (LIMIT/IOC/GTD/...) are instead recovered from
    // timeInForce, per the real OUCH 4.2 sentinel table: 0 = Immediate or
    // Cancel, 99998 = "Market Hours" (auto-cancel at that day's market
    // close — functionally this exchange's own GOOD_TILL_DAY). 99999
    // ("System Hours") and 99996 ("Extended Trading Close") have no clean
    // equivalent in this exchange's ORDER_TYPE model and are deliberately
    // left unmapped rather than guessed at; every other value, including
    // those two, falls through to a plain LIMIT order — a safe, harmless
    // default, not a rejection.
    ORDER_TYPE mapTimeInForce(uint32_t timeInForce) {
        switch (timeInForce) {
        case 0:     return ORDER_TYPE::IMMEDIATE_OR_CANCEL;
        case 99998: return ORDER_TYPE::GOOD_TILL_DAY;
        default:    return ORDER_TYPE::LIMIT;
        }
    }

} // namespace

// ---------------------------------------------------------------------------
// Real NASDAQ OUCH 4.2 wire structs, from OUCH.hpp — see that file for the
// full field layout and spec citation. Frame layout on the wire:
//   [2 bytes, BE] payloadLen  -- length of everything after this field;
//                                equals sizeof() the matching OUCH.hpp
//                                struct below (that struct's own
//                                messageType field included)
//   [1 byte]      msgType     -- 'O' = Enter, 'X' = Cancel, 'U' = Replace
//   [payloadLen-1 bytes]      -- the rest of that same struct
// The [2B len] framing itself is this project's own transport envelope
// (real OUCH runs over SoupBinTCP, deliberately not implemented here) —
// only what comes after it is byte-accurate to the real spec. Each case
// below reinterpret_casts the region starting at the msgType byte directly
// onto the corresponding #pragma pack(1) OUCH.hpp struct (msgType doubles
// as that struct's own first field), then reads every multi-byte integer
// field through the existing readBE16/readBE32 helpers — reinterpret_cast
// recovers field layout, readBE* still handles the wire's big-endianness,
// which this project's own machines don't share. Single-byte/char[N]
// fields need no swap and are read directly off the cast struct.
//
// Error policy, split by whether a command can even be built from the bytes:
//   - Structurally uninterpretable (wrong length for the given type, an
//     unrecognized message type byte): the wire framing itself is broken,
//     nothing downstream could make sense of it either. Returns
//     PARSE_FATAL_ERROR; NetworkGateway closes the connection rather than
//     stalling on bytes that will never parse.
//   - Well-formed frame, invalid content (unknown symbol, price outside
//     this exchange's [WORST_BID, WORST_ASK] range): the frame has the
//     right shape, a real OuchOrderCommand can still be built from what's
//     already parsed. Returns totalFrameLen with type = CommandType::INVALID,
//     same as any other command — OrderBook rejects it through the normal
//     pipeline, matching how CANCEL_ORDER's unknown-token case below is
//     already "accepted, not rejected" at this layer rather than killing
//     the session over a business-level miss.
// ---------------------------------------------------------------------------

size_t OuchProtocolHandler::validateAndParse(const char* buf, size_t availableLen, OuchOrderCommand& outCmd) {
    constexpr size_t LENGTH_FIELD_SIZE = 2;

    if (availableLen < LENGTH_FIELD_SIZE) {
        return 0; // not even enough to read the length field yet
    }

    uint16_t payloadLen = readBE16(buf); // length of (msgType byte + rest of the OUCH.hpp struct)
    size_t totalFrameLen = LENGTH_FIELD_SIZE + payloadLen;

    if (availableLen < totalFrameLen) {
        return 0; // full frame hasn't arrived yet
    }
    if (payloadLen < 1) {
        return PARSE_FATAL_ERROR; // no type byte at all — can't tell what this even is
    }

    char msgType = buf[LENGTH_FIELD_SIZE];

    switch (msgType) {
    case 'O': { // ENTER_ORDER
        if (payloadLen != sizeof(OUCHEnterOrder)) {
            return PARSE_FATAL_ERROR; // wrong length to be a real OUCHEnterOrder
        }
        const OUCHEnterOrder* wire = reinterpret_cast<const OUCHEnterOrder*>(buf + LENGTH_FIELD_SIZE);

        std::memcpy(outCmd.orderToken, wire->orderToken, 14);
        outCmd.buySellIndicator = wire->buySellIndicator;
        outCmd.shares = readBE32(reinterpret_cast<const char*>(&wire->shares));

        uint16_t stockLocate = m_symbolRegistry.lookup(wire->stock);
        if (stockLocate == SymbolRegistry::INVALID_SLOT) {
            // Well-formed frame, invalid content — reject downstream rather
            // than disconnect (see error-policy note above). Bail out here
            // rather than parsing further: orderId is never minted and the
            // token is never registered, same as replace_order's own
            // UNKNOWN_ORDER reject when no real order exists.
            outCmd.type = CommandType::INVALID;
            outCmd.invalidReason = RejectReason::UNKNOWN_SYMBOL;
            return totalFrameLen;
        }
        outCmd.stockLocate = stockLocate; // widens uint16_t -> uint32_t, fine

        uint32_t wirePrice = readBE32(reinterpret_cast<const char*>(&wire->price));
        Price decodedPrice{};
        if (!decodePrice(wirePrice, decodedPrice)) {
            outCmd.type = CommandType::INVALID;
            outCmd.invalidReason = RejectReason::PRICE_OUT_OF_RANGE;
            return totalFrameLen;
        }
        outCmd.price = decodedPrice;

        uint32_t timeInForce = readBE32(reinterpret_cast<const char*>(&wire->timeInForce));
        outCmd.orderType = mapTimeInForce(timeInForce);

        // mpid is a 4-char ASCII firm code, not a numeric id — pack it into
        // a stable integer key the same way SymbolRegistry already packs an
        // 8-byte ticker, rather than standing up a separate firm registry.
        outCmd.firmId = readBE32(wire->mpid);

        // display, capacity, intermarketSweep, minimumQuantity, crossType,
        // customerType are real OUCH fields with no engine feature to
        // consume them yet — intentionally not read. reinterpret_cast means
        // there's no manual offset to "skip past" them either.

        outCmd.orderId = m_nextOrderId++;
        m_orderTokenRegistry.insert(outCmd.orderToken, outCmd.orderId);

        outCmd.type = CommandType::ENTER_ORDER;
        return totalFrameLen;
    }

    case 'X': { // CANCEL_ORDER
        if (payloadLen != sizeof(OUCHCancelOrder)) {
            return PARSE_FATAL_ERROR; // wrong length to be a real OUCHCancelOrder
        }
        const OUCHCancelOrder* wire = reinterpret_cast<const OUCHCancelOrder*>(buf + LENGTH_FIELD_SIZE);

        std::memcpy(outCmd.orderToken, wire->orderToken, 14);
        outCmd.shares = readBE32(reinterpret_cast<const char*>(&wire->shares)); // 0 = cancel all remaining

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
        if (payloadLen != sizeof(OUCHReplaceOrder)) {
            return PARSE_FATAL_ERROR; // wrong length to be a real OUCHReplaceOrder
        }
        const OUCHReplaceOrder* wire = reinterpret_cast<const OUCHReplaceOrder*>(buf + LENGTH_FIELD_SIZE);

        // Not found (INVALID_ORDER_ID) is accepted, not rejected here — the
        // same reasoning as CANCEL_ORDER: OrderBook rejects a replace that
        // references an unknown order, this layer just resolves the token.
        outCmd.orderId = m_orderTokenRegistry.resolve(wire->existingOrderToken);

        std::memcpy(outCmd.orderToken, wire->replacementOrderToken, 14); // the NEW token

        outCmd.shares = readBE32(reinterpret_cast<const char*>(&wire->shares));

        uint32_t wirePrice = readBE32(reinterpret_cast<const char*>(&wire->price));
        Price decodedPrice{};
        if (!decodePrice(wirePrice, decodedPrice)) {
            outCmd.type = CommandType::INVALID;
            outCmd.invalidReason = RejectReason::PRICE_OUT_OF_RANGE;
            return totalFrameLen;
        }
        outCmd.price = decodedPrice;

        // timeInForce/display/intermarketSweep/minimumQuantity are present
        // in the real struct but not read: OrderBook::replace_order never
        // consults cmd.orderType (it keeps the existing order's own type),
        // so there is nothing to map them to here.

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
        return PARSE_FATAL_ERROR; // no idea how to interpret the body at all
    }
}
