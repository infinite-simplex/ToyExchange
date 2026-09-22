#pragma once
#include <cstring>
#include <cstdint>
#include "SymbolRegistry.hpp"
#include "OrderTokenRegistry.hpp"
#include "OuchOrderCommand.hpp"

class OuchProtocolHandler {
public:
    OuchProtocolHandler(const SymbolRegistry& symbolRegistry, OrderTokenRegistry& orderTokenRegistry)
        : m_symbolRegistry(symbolRegistry), m_orderTokenRegistry(orderTokenRegistry) {}
    size_t validateAndParse(const char* buf, size_t availableLen, OuchOrderCommand& outCmd);
private:
    const SymbolRegistry& m_symbolRegistry;
    OrderTokenRegistry& m_orderTokenRegistry; // single ingress thread only — see OrderTokenRegistry.hpp
    OrderId m_nextOrderId{ 1 };               // incremented only for ENTER_ORDER frames
};