#pragma once
#include <Alias.hpp>
#include <OrderTypes.hpp>

#define INVALID_PRICE UINT8_MAX
#define INVALID_IDX UINT32_MAX

class Order {
public:
	Order() : m_id(0) {}

	Order(OrderId id, FirmId firm_id, SIDE side, ORDER_TYPE type, Price price, Quantity quantity)
		: m_id(id),
		m_firm_id(firm_id),
		m_initial_quantity(quantity),
		m_remaining_quantity(quantity),
		m_type(type),
		m_side(side),
		m_price(price) {
	}

	[[nodiscard]] OrderId get_id() const noexcept { return m_id; }
	[[nodiscard]] FirmId get_firm_id() const noexcept { return m_firm_id; }
	[[nodiscard]] SIDE get_side() const noexcept { return m_side; }
	[[nodiscard]] ORDER_TYPE get_type() const noexcept { return m_type; }
	[[nodiscard]] Price get_price() const noexcept { return m_price; }
	[[nodiscard]] Quantity get_initial_quantity() const noexcept { return m_initial_quantity; }
	[[nodiscard]] Quantity get_remaining_quantity() const noexcept { return m_remaining_quantity; }

	[[nodiscard]] Quantity fill(Order& order) noexcept {
		Quantity filledAmount = (((m_remaining_quantity) < (order.m_remaining_quantity)) ? (m_remaining_quantity) : (order.m_remaining_quantity));
		m_remaining_quantity -= filledAmount;
		order.m_remaining_quantity -= filledAmount;
		return filledAmount;
	}

	[[nodiscard]] bool is_filled() const noexcept { return m_remaining_quantity == 0; }
	void set_price(Price newPrice) noexcept { m_price = newPrice; }
	void cancel() noexcept { m_remaining_quantity = 0; }

	

private:
	//Exchange given Id
	OrderId m_id{ 0 };
	Price m_price{ 0u };
public:
	// 4-byte aligned
	PoolIdx m_pool_idx{ INVALID_IDX };
	PoolIdx m_prev_idx{ INVALID_IDX };
	PoolIdx m_next_idx{ INVALID_IDX };
	TraceId m_telemetry_idx{ 0 };
	SessionId m_session_id{ INVALID_SESSION_ID }; // owning connection, carried through to OrderEvent for egress routing
private:
	FirmId m_firm_id{ 0 };
	Quantity m_initial_quantity{ 0 };
	Quantity m_remaining_quantity{ 0 };
	ORDER_TYPE m_type{ ORDER_TYPE::NONE };
	SIDE m_side{ SIDE::NO_SIDE };
};

static_assert(sizeof(Order) <= 56, "Order grew past its expected pool-friendly size budget");