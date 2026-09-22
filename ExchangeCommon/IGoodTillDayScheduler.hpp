#pragma once
#include <chrono>

// Lives in ExchangeCommon (not MatchingService) so both MatchingService
// (which calls it, from OrderBook) and NetworkService (which implements it,
// via GtdCancelService) can depend on it without either depending on the
// other — mirrors why ORDER_TYPE/SIDE live here too.
//
// Injected into OrderBook by pointer, not a template param — GTD scheduling
// is a rare, non-hot-path event, unlike the telemetry/matching policies that
// fire on every order, so virtual dispatch here is a non-issue and this keeps
// OrderBook<TelemetryPolicy>'s existing signature untouched.
class IGoodTillDayScheduler {
public:
    virtual ~IGoodTillDayScheduler() = default;

    // Called once, from the matching thread, when a GOOD_TILL_DAY order rests
    // with quantity remaining. Implementations must not block the caller.
    virtual void schedule_cancel(const char orderToken[14],
        std::chrono::system_clock::time_point cancelAt) = 0;
};
