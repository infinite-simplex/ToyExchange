#include "MatchingService.hpp"
#include "OrderBook.hpp"
#include "OuchOrderCommand.hpp"
#include "SPSCProducerPolicy.hpp"

// Explicit instantiation for the OUCH pipeline: keeps this translation unit
// non-empty for the STATIC library target and compile-checks that
// MatchingService<TCommand, TEngine> actually builds against
// OrderBook::submit_order for this command type.
template class MatchingService<OuchOrderCommand, OrderBook<SPSCProducerPolicy<OrderEvent>>>;
