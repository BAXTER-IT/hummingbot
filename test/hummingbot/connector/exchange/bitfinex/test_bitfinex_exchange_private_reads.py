"""The two private REST reads the base class polls: an order's status and its fills. They matter after the
midnight session reset, when FIX cannot say what happened to an order that was resting."""

import json
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.connector.exchange.bitfinex import bitfinex_constants as CONSTANTS, bitfinex_web_utils as web_utils
from hummingbot.connector.exchange.bitfinex.bitfinex_exchange import BitfinexExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

# Bitfinex order array: [ID, GID, CID, SYMBOL, MTS_CREATE, MTS_UPDATE, AMOUNT, AMOUNT_ORIG, TYPE, TYPE_PREV, _, _,
#                        FLAGS, STATUS, _, _, PRICE, PRICE_AVG, ...]
def order_row(order_id, status, amount, amount_orig, price, price_avg="0", symbol="tBTCUST"):
    return [order_id, None, 9001, symbol, 1700000000000, 1700000001000, amount, amount_orig, "EXCHANGE LIMIT", None,
            None, None, 4096, status, None, None, price, price_avg, 0, 0, None, None, None, 0, 0, None, None, None,
            "API>BFX", None, None, {}]


# Bitfinex trade array: [ID, PAIR, MTS_CREATE, ORDER_ID, EXEC_AMOUNT, EXEC_PRICE, ORDER_TYPE, ORDER_PRICE, MAKER, FEE, FEE_CURRENCY]
def trade_row(trade_id, order_id, amount, price, maker=1, fee="-0.000001", fee_ccy="BTC"):
    return [trade_id, "tBTCUST", 1700000002000, order_id, amount, price, "EXCHANGE LIMIT", 77000, maker, fee, fee_ccy]


class TestBitfinexPrivateReads(IsolatedAsyncioWrapperTestCase):

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.exchange = BitfinexExchange(
            bitfinex_api_key="k", bitfinex_secret_key="s", bitfinex_fix_username="", bitfinex_fix_password="",
            bitfinex_fix_sender_comp_id="", trading_pairs=["BTC-USDT"], trading_required=False)
        self.exchange._set_trading_pair_symbol_map(bidict({"tBTCUST": "BTC-USDT"}))
        self.exchange.start_tracking_order(order_id="HB-9", exchange_order_id="555", trading_pair="BTC-USDT",
                                           trade_type=TradeType.BUY, price=Decimal("77000"), amount=Decimal("0.001"),
                                           order_type=OrderType.LIMIT_MAKER)
        self.order = self.exchange._order_tracker.fetch_order(client_order_id="HB-9")

    @aioresponses()
    async def test_an_active_order_reads_as_open(self, mock_api):
        mock_api.post(web_utils.private_rest_url(CONSTANTS.ACTIVE_ORDERS_PATH.format(symbol="tBTCUST")),
                      body=json.dumps([order_row(555, "ACTIVE", 0.001, 0.001, 77000)]))
        update = await self.exchange._request_order_status(self.order)
        self.assertEqual(OrderState.OPEN, update.new_state)
        self.assertEqual("555", update.exchange_order_id)

    @aioresponses()
    async def test_a_finished_order_is_looked_up_in_history_and_reads_its_final_state(self, mock_api):
        mock_api.post(web_utils.private_rest_url(CONSTANTS.ACTIVE_ORDERS_PATH.format(symbol="tBTCUST")), body="[]")
        mock_api.post(web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH.format(symbol="tBTCUST")),
                      body=json.dumps([order_row(555, "EXECUTED @ 77000.0(0.001)", 0, 0.001, 77000, "77000")]))
        update = await self.exchange._request_order_status(self.order)
        self.assertEqual(OrderState.FILLED, update.new_state)
        mock_api.post(web_utils.private_rest_url(CONSTANTS.ACTIVE_ORDERS_PATH.format(symbol="tBTCUST")), body="[]")
        mock_api.post(web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH.format(symbol="tBTCUST")),
                      body=json.dumps([order_row(555, "CANCELED", 0.001, 0.001, 77000)]))
        self.assertEqual(OrderState.CANCELED, (await self.exchange._request_order_status(self.order)).new_state)
        mock_api.post(web_utils.private_rest_url(CONSTANTS.ACTIVE_ORDERS_PATH.format(symbol="tBTCUST")), body="[]")
        mock_api.post(web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH.format(symbol="tBTCUST")),
                      body=json.dumps([order_row(555, "PARTIALLY FILLED @ 77000.0(0.0004)", 0.0006, 0.001, 77000, "77000")]))
        self.assertEqual(OrderState.PARTIALLY_FILLED, (await self.exchange._request_order_status(self.order)).new_state)

    @aioresponses()
    async def test_an_order_known_nowhere_is_reported_as_not_found(self, mock_api):
        mock_api.post(web_utils.private_rest_url(CONSTANTS.ACTIVE_ORDERS_PATH.format(symbol="tBTCUST")), body="[]")
        mock_api.post(web_utils.private_rest_url(CONSTANTS.ORDER_HISTORY_PATH.format(symbol="tBTCUST")), body="[]")
        with self.assertRaises(IOError) as err:
            await self.exchange._request_order_status(self.order)
        self.assertTrue(self.exchange._is_order_not_found_during_status_update_error(err.exception))

    @aioresponses()
    async def test_fills_come_with_maker_flag_and_the_venues_fee(self, mock_api):
        mock_api.post(web_utils.private_rest_url(CONSTANTS.ORDER_TRADES_PATH.format(symbol="tBTCUST", order_id="555")),
                      body=json.dumps([trade_row(77, 555, 0.0004, 77000, maker=1, fee="-0.0000004", fee_ccy="BTC"),
                                       trade_row(78, 555, 0.0006, 77001, maker=-1, fee="-0.09", fee_ccy="UST")]))
        updates = await self.exchange._all_trade_updates_for_order(self.order)
        self.assertEqual(["77", "78"], [u.trade_id for u in updates])
        self.assertEqual(Decimal("0.0004"), updates[0].fill_base_amount)
        self.assertEqual(Decimal("77000"), updates[0].fill_price)
        self.assertFalse(updates[0].is_taker)
        self.assertTrue(updates[1].is_taker)
        self.assertEqual(Decimal("0.0000004"), updates[0].fee.flat_fees[0].amount)
        self.assertEqual("BTC", updates[0].fee.flat_fees[0].token)
        self.assertEqual("USDT", updates[1].fee.flat_fees[0].token)  # UST reported as USDT
        self.assertEqual(1700000002.0, updates[0].fill_timestamp)

    def test_the_authenticator_is_the_bitfinex_one(self):
        from hummingbot.connector.exchange.bitfinex.bitfinex_auth import BitfinexAuth
        self.assertIsInstance(self.exchange.authenticator, BitfinexAuth)
