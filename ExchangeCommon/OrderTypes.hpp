#pragma once

// Shared domain vocabulary between NetworkService (wire commands) and
// MatchingService (the book) — lives here, not in MatchingService/Order.hpp,
// so ExchangeCommon (which NetworkService and MatchingService both depend on)
// never has to depend back on MatchingService.

enum class ORDER_TYPE {
	LIMIT,
	CANCEL,
	IMMEDIATE_OR_CANCEL,
	MODIFY,
	FILL_OR_KILL,
	GOOD_TILL_CANCEL,
	GOOD_TILL_DAY,
	MARKET,
	NONE
};

enum class SIDE {
	BID,
	ASK,
	NO_SIDE
};
