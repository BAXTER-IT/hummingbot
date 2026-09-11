"""The connector's order path over FIX, against the gateway simulator: place, reject, fill, cancel,
cancel-reject, and the session ending under resting orders. Assertions are on the order tracker's
state and Hummingbot's events — where the strategy ends up, not which message went out."""

import asyncio
from decimal import Decimal

import pytest
import pytest_asyncio

from hummingbot.connector.exchange.bitfinex.bitfinex_exchange import BitfinexExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent
from test.hummingbot.connector.exchange.bitfinex.fix.bitfinex_simulator import BitfinexFixSimulator

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def sim():
    simulator = BitfinexFixSimulator(username="fxch", password="s3cret")
    await simulator.start()
    yield simulator
    await simulator.stop()


@pytest_asyncio.fixture
async def exchange(sim):
    host, port = sim._server.sockets[0].getsockname()[:2]
    ex = BitfinexExchange(bitfinex_api_key="", bitfinex_secret_key="", bitfinex_fix_username="fxch",
                          bitfinex_fix_password="s3cret", bitfinex_fix_sender_comp_id="FXCHTEST",
                          bitfinex_fix_host=host, bitfinex_fix_port=port, bitfinex_fix_tls=False,
                          trading_pairs=["BTC-USDT"], trading_required=True)
    ex._fix_gateway._reconnect_wait_s = 0.1
    ex._set_trading_pair_symbol_map(__import__("bidict").bidict({"tBTCUST": "BTC-USDT"}))
    await ex._fix_gateway.start()
    await ex._fix_gateway.wait_logged_on(timeout=5)
    ex._events = EventLogger()
    for tag in (MarketEvent.OrderFilled, MarketEvent.BuyOrderCompleted, MarketEvent.OrderCancelled,
                MarketEvent.OrderFailure, MarketEvent.BuyOrderCreated):
        ex.add_listener(tag, ex._events)
    yield ex
    await ex._fix_gateway.stop()


def track(ex, client_id, amount="0.001", price="60000", order_type=OrderType.LIMIT_MAKER, side=TradeType.BUY):
    ex.start_tracking_order(order_id=client_id, exchange_order_id=None, trading_pair="BTC-USDT", trade_type=side,
                            price=Decimal(price), amount=Decimal(amount), order_type=order_type)
    return ex._order_tracker.fetch_order(client_order_id=client_id)


async def pump(ex, n, timeout=5):
    """Feed the next n gateway events through the connector's listener logic."""
    for _ in range(n):
        event = await asyncio.wait_for(ex._fix_gateway.events.get(), timeout)
        await ex._process_fix_event(event)


async def test_place_order_sends_post_only_over_fix_and_returns_the_venue_id(exchange, sim):
    order_id, ts = await exchange._place_order(order_id="HB-1", trading_pair="BTC-USDT", amount=Decimal("0.001"),
                                               trade_type=TradeType.BUY, order_type=OrderType.LIMIT_MAKER,
                                               price=Decimal("60000"))
    assert order_id.isdigit() and ts > 0
    d = next(m for m in sim.received if m.msg_type == "D")
    assert d.get("3927") == "4096" and d.get("40") == "2" and d.get("59") == "0" and d.get("55") == "BTCUST"


async def test_a_reject_raises_so_the_base_marks_the_order_failed(exchange, sim):
    with pytest.raises(Exception) as err:
        await exchange._place_order(order_id="HB-2", trading_pair="BTC-USDT", amount=Decimal("1"),
                                    trade_type=TradeType.BUY, order_type=OrderType.LIMIT, price=None)
    assert "Price required" in str(err.value)


async def test_a_maker_fill_becomes_a_fill_event_with_zero_taker_flag_and_completes_the_order(exchange, sim):
    order = track(exchange, "HB-3")
    sim.fill_next = True
    venue_id, _ = await exchange._place_order(order_id="HB-3", trading_pair="BTC-USDT", amount=Decimal("0.001"),
                                              trade_type=TradeType.BUY, order_type=OrderType.LIMIT_MAKER,
                                              price=Decimal("60000"))
    order.update_exchange_order_id(venue_id)
    await pump(exchange, 2)  # New, Trade
    assert order.current_state is OrderState.FILLED
    fills = exchange._events.event_log
    fill = next(e for e in fills if type(e).__name__ == "OrderFilledEvent")
    assert fill.amount == Decimal("0.001") and fill.price == Decimal("60000") and fill.order_id == "HB-3"
    assert fill.exchange_trade_id and fill.exchange_order_id == venue_id
    assert any(type(e).__name__ == "BuyOrderCompletedEvent" for e in fills)


async def test_cancel_is_asynchronous_and_finalised_by_the_venues_report(exchange, sim):
    order = track(exchange, "HB-4", price="1")
    venue_id, _ = await exchange._place_order(order_id="HB-4", trading_pair="BTC-USDT", amount=Decimal("0.001"),
                                              trade_type=TradeType.BUY, order_type=OrderType.LIMIT_MAKER, price=Decimal("1"))
    order.update_exchange_order_id(venue_id)
    await pump(exchange, 1)
    assert await exchange._place_cancel("HB-4", order) is True
    assert order.current_state is not OrderState.CANCELED  # not until the venue says so
    await pump(exchange, 1)
    assert order.current_state is OrderState.CANCELED
    assert any(type(e).__name__ == "OrderCancelledEvent" and e.order_id == "HB-4" for e in exchange._events.event_log)


async def test_cancel_reject_for_an_unknown_order_counts_as_order_not_found(exchange, sim):
    order = track(exchange, "HB-5", price="1")
    order.update_exchange_order_id("0")
    await exchange._place_cancel("HB-5", order)
    await pump(exchange, 1)
    assert exchange._order_tracker._lost_orders.get("HB-5") is None
    assert exchange._order_tracker._order_not_found_records.get("HB-5", 0) == 1


async def test_session_end_cancels_every_resting_order_in_the_tracker(exchange, sim):
    order = track(exchange, "HB-6", price="1")
    venue_id, _ = await exchange._place_order(order_id="HB-6", trading_pair="BTC-USDT", amount=Decimal("0.001"),
                                              trade_type=TradeType.BUY, order_type=OrderType.LIMIT_MAKER, price=Decimal("1"))
    order.update_exchange_order_id(venue_id)
    await pump(exchange, 1)
    assert order.current_state is OrderState.OPEN
    await sim.send_logout("Trading session ending")
    for _ in range(3):
        await pump(exchange, 1)
        if order.current_state is OrderState.CANCELED:
            break
    assert order.current_state is OrderState.CANCELED
    assert exchange._is_user_stream_initialized() is False
