"""The order gateway on top of the session: Hummingbot orders in, execution reports out."""

import asyncio
from decimal import Decimal

import pytest
import pytest_asyncio

from hummingbot.connector.exchange.bitfinex.bitfinex_fix_gateway import BitfinexFixGateway, FixOrderRejected
from test.hummingbot.connector.exchange.bitfinex.fix.bitfinex_simulator import BitfinexFixSimulator

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def sim():
    simulator = BitfinexFixSimulator(username="fxch", password="s3cret")
    await simulator.start()
    yield simulator
    await simulator.stop()


@pytest_asyncio.fixture
async def gateway(sim):
    host, port = sim._server.sockets[0].getsockname()[:2]
    gw = BitfinexFixGateway(host=host, port=port, sender_comp_id="FXCHTEST", username="fxch", password="s3cret",
                            heartbeat_s=1.0, use_tls=False, reconnect_wait_s=0.1, log=lambda _: None)
    await gw.start()
    await gw.wait_logged_on(timeout=5)
    yield gw
    await gw.stop()


async def next_event(gw, kind, timeout=5):
    async def find():
        while True:
            event = await gw.events.get()
            if event["kind"] == kind:
                return event
    return await asyncio.wait_for(find(), timeout)


async def test_post_only_limit_order_is_accepted_and_returns_the_venue_order_id(gateway, sim):
    order_id, ts = await gateway.place_order(client_order_id="HBOT-1", trading_pair="BTC-USDT", is_buy=True,
                                             amount=Decimal("0.001"), price=Decimal("60000"), order_type="limit",
                                             post_only=True)
    assert order_id.isdigit() and ts > 0
    d = next(m for m in sim.received if m.msg_type == "D")
    assert d.get("11") == "HBOT-1" and d.get("55") == "BTCUST" and d.get("54") == "1" and d.get("38") == "0.001"
    assert d.get("40") == "2" and d.get("44") == "60000" and d.get("59") == "0" and d.get("3927") == "4096"
    assert d.get("6061") == "N"


async def test_market_sell_has_no_price_and_no_post_only_flag(gateway, sim):
    await gateway.place_order(client_order_id="HBOT-2", trading_pair="ETH-USDT", is_buy=False, amount=Decimal("0.5"),
                              price=None, order_type="market", post_only=False)
    d = next(m for m in sim.received if m.msg_type == "D" and m.get("11") == "HBOT-2")
    assert d.get("40") == "1" and d.get("44") is None and d.get("3927") is None and d.get("54") == "2"


async def test_a_venue_reject_raises_with_the_venues_text(gateway, sim):
    with pytest.raises(FixOrderRejected) as err:
        await gateway.place_order(client_order_id="HBOT-3", trading_pair="BTC-USDT", is_buy=True, amount=Decimal("1"),
                                  price=None, order_type="limit", post_only=False)  # limit without price → rejected
    assert "Price required" in str(err.value)


async def test_a_fill_arrives_as_an_execution_report_event_with_maker_flag(gateway, sim):
    sim.fill_next = True
    await gateway.place_order(client_order_id="HBOT-4", trading_pair="BTC-USDT", is_buy=True, amount=Decimal("0.002"),
                              price=Decimal("50000"), order_type="limit", post_only=True)
    fill = None
    for _ in range(3):
        event = await next_event(gateway, "execution_report")
        if event["exec_type"] == "F":
            fill = event
            break
    assert fill is not None
    assert fill["client_order_id"] == "HBOT-4" and fill["last_qty"] == Decimal("0.002") and fill["last_px"] == Decimal("50000")
    assert fill["is_maker"] is True and fill["cum_qty"] == Decimal("0.002") and fill["leaves_qty"] == Decimal("0")
    assert fill["ord_status"] == "2" and fill["exec_id"]


async def test_cancel_goes_by_the_last_client_id_and_the_report_maps_back_to_the_order(gateway, sim):
    order_id, _ = await gateway.place_order(client_order_id="HBOT-5", trading_pair="BTC-USDT", is_buy=True,
                                            amount=Decimal("1"), price=Decimal("1"), order_type="limit", post_only=True)
    await next_event(gateway, "execution_report")  # the New
    await gateway.cancel_order(client_order_id="HBOT-5", exchange_order_id=order_id)
    await sim.wait_for(lambda r: any(m.msg_type == "F" for m in r), timeout=3)
    f = next(m for m in sim.received if m.msg_type == "F")
    assert f.get("41") == "HBOT-5" and f.get("37") == order_id and f.get("11") != "HBOT-5" and len(f.get("11")) <= 36
    canceled = await next_event(gateway, "execution_report")
    assert canceled["exec_type"] == "4" and canceled["client_order_id"] == "HBOT-5"


async def test_cancelling_an_unknown_order_is_a_cancel_reject_event(gateway, sim):
    await gateway.cancel_order(client_order_id="HBOT-ghost", exchange_order_id="0")
    reject = await next_event(gateway, "cancel_reject")
    assert reject["client_order_id"] == "HBOT-ghost" and reject["reason"] == "1" and "Unknown" in reject["text"]


async def test_pre_close_refuses_new_orders_until_the_next_logon(gateway, sim):
    await sim.send_pre_close()
    await next_event(gateway, "pre_close")
    with pytest.raises(FixOrderRejected) as err:
        await gateway.place_order(client_order_id="HBOT-6", trading_pair="BTC-USDT", is_buy=True, amount=Decimal("1"),
                                  price=Decimal("1"), order_type="limit", post_only=True)
    assert "pre-close" in str(err.value).lower()


async def test_a_dropped_line_is_reported_and_the_gateway_logs_on_again(gateway, sim):
    await sim.drop_connection()
    event = await next_event(gateway, "disconnected")
    assert event["text"]
    await gateway.wait_logged_on(timeout=5)
    assert sim.logons == 2 and gateway.logged_on
