"""Order book over Bitfinex's public websocket (book P0 + trades) with a REST snapshot to seed."""

import asyncio
import json
from pathlib import Path
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.connector.exchange.bitfinex import bitfinex_constants as CONSTANTS, bitfinex_web_utils as web_utils
from hummingbot.connector.exchange.bitfinex.bitfinex_api_order_book_data_source import BitfinexAPIOrderBookDataSource
from hummingbot.connector.exchange.bitfinex.bitfinex_exchange import BitfinexExchange
from hummingbot.core.data_type.order_book_message import OrderBookMessageType

FIXTURES = Path(__file__).parent / "fixtures"


class TestBitfinexAPIOrderBookDataSource(IsolatedAsyncioWrapperTestCase):

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.connector = BitfinexExchange(
            bitfinex_api_key="", bitfinex_secret_key="", bitfinex_fix_username="", bitfinex_fix_password="",
            bitfinex_fix_sender_comp_id="", bitfinex_fix_host="", bitfinex_fix_port=0,
            trading_pairs=["BTC-USDT"], trading_required=False,
        )
        self.connector._set_trading_pair_symbol_map(bidict({"tBTCUST": "BTC-USDT"}))
        self.ds = BitfinexAPIOrderBookDataSource(trading_pairs=["BTC-USDT"], connector=self.connector,
                                                 api_factory=self.connector._web_assistants_factory)

    @aioresponses()
    async def test_snapshot_splits_bids_and_asks_by_the_sign_of_the_amount(self, mock_api):
        mock_api.get(web_utils.public_rest_url(CONSTANTS.BOOK_PATH.format(symbol="tBTCUST")) + f"?len={CONSTANTS.BOOK_DEPTH}",
                     body=(FIXTURES / "book_tBTCUST_P0.json").read_text())
        rows = json.loads((FIXTURES / "book_tBTCUST_P0.json").read_text())
        expected_bids = sorted((float(p) for p, c, a in rows if a > 0), reverse=True)
        expected_asks = sorted(float(p) for p, c, a in rows if a < 0)
        msg = await self.ds._order_book_snapshot("BTC-USDT")
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual("BTC-USDT", msg.trading_pair)
        self.assertEqual(expected_bids, [float(b.price) for b in msg.bids])
        self.assertEqual(expected_asks, [float(a.price) for a in msg.asks])
        self.assertTrue(len(expected_bids) == 4 and len(expected_asks) == 4)
        self.assertTrue(all(float(a.amount) > 0 for a in msg.asks))  # sign removed
        self.assertGreater(msg.update_id, 0)

    async def test_subscribe_sends_book_and_trades_subscriptions_per_pair(self):
        ws = AsyncMock()
        await self.ds._subscribe_channels(ws)
        payloads = [call.args[0].payload for call in ws.send.call_args_list]
        self.assertEqual(2, len(payloads))
        self.assertIn({"event": "subscribe", "channel": "book", "symbol": "tBTCUST", "prec": "P0", "freq": "F0",
                       "len": str(CONSTANTS.BOOK_DEPTH)}, payloads)
        self.assertIn({"event": "subscribe", "channel": "trades", "symbol": "tBTCUST"}, payloads)

    async def test_channel_ids_are_learned_from_subscribed_events_and_frames_are_routed(self):
        self.ds._register_subscription({"event": "subscribed", "channel": "book", "chanId": 10, "symbol": "tBTCUST"})
        self.ds._register_subscription({"event": "subscribed", "channel": "trades", "chanId": 11, "symbol": "tBTCUST"})
        self.assertEqual(self.ds._diff_messages_queue_key, self.ds._channel_originating_message([10, [77128, 1, 0.5]]))
        self.assertEqual(self.ds._snapshot_messages_queue_key,
                         self.ds._channel_originating_message([10, [[77128, 1, 0.5], [77130, 1, -0.2]]]))
        self.assertEqual(self.ds._trade_messages_queue_key,
                         self.ds._channel_originating_message([11, "te", [1, 1700000000000, 0.01, 77128]]))
        self.assertEqual("", self.ds._channel_originating_message([10, "hb"]))
        self.assertEqual("", self.ds._channel_originating_message([11, "tu", [1, 1700000000000, 0.01, 77128]]))
        self.assertEqual("", self.ds._channel_originating_message({"event": "info", "version": 2}))

    async def test_diff_parsing_add_change_and_delete_levels(self):
        self.ds._register_subscription({"event": "subscribed", "channel": "book", "chanId": 10, "symbol": "tBTCUST"})
        q = asyncio.Queue()
        await self.ds._parse_order_book_diff_message([10, [77128, 2, 0.75]], q)      # bid level update
        await self.ds._parse_order_book_diff_message([10, [77190, 1, -0.10]], q)     # ask level update
        await self.ds._parse_order_book_diff_message([10, [77128, 0, 1]], q)         # bid level delete
        await self.ds._parse_order_book_diff_message([10, [77190, 0, -1]], q)        # ask level delete
        msgs = [q.get_nowait() for _ in range(4)]
        self.assertEqual([OrderBookMessageType.DIFF] * 4, [m.type for m in msgs])
        self.assertEqual([(77128.0, 0.75)], [(float(b.price), float(b.amount)) for b in msgs[0].bids])
        self.assertEqual([(77190.0, 0.10)], [(float(a.price), float(a.amount)) for a in msgs[1].asks])
        self.assertEqual([(77128.0, 0.0)], [(float(b.price), float(b.amount)) for b in msgs[2].bids])
        self.assertEqual([(77190.0, 0.0)], [(float(a.price), float(a.amount)) for a in msgs[3].asks])
        self.assertTrue(msgs[0].update_id < msgs[1].update_id < msgs[2].update_id < msgs[3].update_id)

    async def test_snapshot_over_the_websocket_is_a_snapshot_message(self):
        self.ds._register_subscription({"event": "subscribed", "channel": "book", "chanId": 10, "symbol": "tBTCUST"})
        q = asyncio.Queue()
        await self.ds._parse_order_book_snapshot_message([10, [[77128, 1, 0.5], [77130, 1, -0.2]]], q)
        msg = q.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual([77128.0], [float(b.price) for b in msg.bids])
        self.assertEqual([77130.0], [float(a.price) for a in msg.asks])

    async def test_trade_parsing_takes_side_from_the_amounts_sign(self):
        self.ds._register_subscription({"event": "subscribed", "channel": "trades", "chanId": 11, "symbol": "tBTCUST"})
        q = asyncio.Queue()
        await self.ds._parse_trade_message([11, "te", [555, 1700000000123, -0.02, 77130]], q)
        msg = q.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual("BTC-USDT", msg.trading_pair)
        self.assertEqual(2.0, msg.content["trade_type"])  # TradeType.SELL
        self.assertEqual(0.02, msg.content["amount"]) if isinstance(msg.content["amount"], float) \
            else self.assertEqual("0.02", str(msg.content["amount"]))
        self.assertEqual("555", msg.content["trade_id"])
        self.assertEqual(1700000000.123, msg.timestamp)
