#include "MatchingService.hpp"
#include "OuchOrderCommand.hpp"

// Explicit instantiation for the OUCH pipeline: keeps this translation unit
// non-empty for the STATIC library target and compile-checks that
// MatchingService<TCommand> actually builds against OrderBook::submit_order
// for this command type.
template class MatchingService<OuchOrderCommand>;
