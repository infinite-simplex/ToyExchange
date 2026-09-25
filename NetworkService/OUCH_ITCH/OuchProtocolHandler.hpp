#pragma once
#include <cstring>
#include <cstdint>
#include "SymbolRegistry.hpp"
#include "OrderTokenRegistry.hpp"
#include "OuchOrderCommand.hpp"

class OuchProtocolHandler {
public:
    // Sentinel return from validateAndParse, distinct from 0 ("not enough
    // bytes yet — wait for more") and any real byte count: the buffer holds
    // a COMPLETE frame that is structurally uninterpretable (bad body
    // length for its type, or an unrecognized message type byte) — no
    // OuchOrderCommand can be built from it at all, and nothing about
    // future bytes on this connection can be trusted either. The caller
    // (NetworkGateway) is expected to close the connection, not retry.
    static constexpr size_t PARSE_FATAL_ERROR = static_cast<size_t>(-1);

    OuchProtocolHandler(const SymbolRegistry& symbolRegistry, OrderTokenRegistry& orderTokenRegistry)
        : m_symbolRegistry(symbolRegistry), m_orderTokenRegistry(orderTokenRegistry) {}
    size_t validateAndParse(const char* buf, size_t availableLen, OuchOrderCommand& outCmd);
private:
    const SymbolRegistry& m_symbolRegistry;
    OrderTokenRegistry& m_orderTokenRegistry; // single ingress thread only — see OrderTokenRegistry.hpp
    OrderId m_nextOrderId{ 1 };               // incremented only for ENTER_ORDER frames
};