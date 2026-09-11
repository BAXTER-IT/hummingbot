"""Order book and public trades from Bitfinex's public websocket v2 (``book`` P0/F0 and ``trades``
channels), seeded by a REST snapshot. Bitfinex has no sequence numbers on the book feed, so the
data source numbers messages itself with a monotonic counter: the tracker only needs ordering."""

import asyncio
import itertools
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.bitfinex import (
    bitfinex_constants as CONSTANTS,
    bitfinex_utils as utils,
    bitfinex_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitfinex.bitfinex_exchange import BitfinexExchange


class BitfinexAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(self, trading_pairs: List[str], connector: "BitfinexExchange", api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._channels: Dict[int, tuple] = {}  # chanId -> (channel name, trading pair)
        self._update_ids = itertools.count(int(time.time() * 1000))
        self._last_ping = 0.0

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    # -- REST snapshot ------------------------------------------------------------------
    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        rows = await self._request_order_book_snapshot(trading_pair)
        bids, asks = self._split_levels(rows)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": trading_pair, "update_id": next(self._update_ids), "bids": bids, "asks": asks,
        }, time.time())

    async def _request_order_book_snapshot(self, trading_pair: str) -> List[List[float]]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest_assistant = await self._api_factory.get_rest_assistant()
        return await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.BOOK_PATH.format(symbol=symbol), domain=self._domain),
            params={"len": CONSTANTS.BOOK_DEPTH}, method=RESTMethod.GET, throttler_limit_id=CONSTANTS.BOOK_PATH,
        )

    @staticmethod
    def _split_levels(rows: List[List[float]]):
        """P0 rows are [price, count, amount]: amount > 0 is a bid, < 0 an ask; count 0 removes the level."""
        bids, asks = [], []
        for price, count, amount in rows:
            size = 0.0 if count == 0 else abs(float(amount))
            (bids if float(amount) > 0 else asks).append([float(price), size])
        bids.sort(key=lambda level: -level[0])
        asks.sort(key=lambda level: level[0])
        return bids, asks

    # -- websocket ----------------------------------------------------------------------
    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.PUBLIC_WS_URL, ping_timeout=CONSTANTS.WS_PING_INTERVAL_S)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                await ws.send(WSJSONRequest(payload={"event": "subscribe", "channel": CONSTANTS.WS_BOOK_CHANNEL,
                                                     "symbol": symbol, "prec": "P0", "freq": "F0",
                                                     "len": str(CONSTANTS.BOOK_DEPTH)}))
                await ws.send(WSJSONRequest(payload={"event": "subscribe", "channel": CONSTANTS.WS_TRADES_CHANNEL,
                                                     "symbol": symbol}))
            self.logger().info("Subscribed to Bitfinex public book and trades channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error subscribing to Bitfinex order book channels...")
            raise

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        ws = getattr(self, "_ws_assistant", None)
        if ws is None:
            self.logger().warning(f"Cannot subscribe to {trading_pair}: websocket not connected")
            return False
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            await ws.send(WSJSONRequest(payload={"event": "subscribe", "channel": CONSTANTS.WS_BOOK_CHANNEL,
                                                 "symbol": symbol, "prec": "P0", "freq": "F0",
                                                 "len": str(CONSTANTS.BOOK_DEPTH)}))
            await ws.send(WSJSONRequest(payload={"event": "subscribe", "channel": CONSTANTS.WS_TRADES_CHANNEL,
                                                 "symbol": symbol}))
            if trading_pair not in self._trading_pairs:
                self._trading_pairs.append(trading_pair)
            return True
        except Exception:  # noqa: BLE001 — report, the tracker decides what to do
            self.logger().exception(f"Error subscribing to {trading_pair}")
            return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        ws = getattr(self, "_ws_assistant", None)
        if ws is None:
            self.logger().warning(f"Cannot unsubscribe from {trading_pair}: websocket not connected")
            return False
        try:
            for chan_id, (_, pair) in list(self._channels.items()):
                if pair == trading_pair:
                    await ws.send(WSJSONRequest(payload={"event": "unsubscribe", "chanId": chan_id}))
            if trading_pair in self._trading_pairs:
                self._trading_pairs.remove(trading_pair)
            return True
        except Exception:  # noqa: BLE001
            self.logger().exception(f"Error unsubscribing from {trading_pair}")
            return False

    def _register_subscription(self, event: Dict[str, Any]) -> None:
        if event.get("event") == "subscribed" and "chanId" in event:
            symbol = event.get("symbol") or event.get("pair")
            trading_pair = self._pair_for_symbol(symbol)
            self._channels[int(event["chanId"])] = (event.get("channel"), trading_pair)
        elif event.get("event") == "unsubscribed" and "chanId" in event:
            self._channels.pop(int(event["chanId"]), None)

    @staticmethod
    def _pair_for_symbol(symbol: str) -> str:
        return utils.trading_pair_from_symbol(symbol)

    def _channel_originating_message(self, event_message) -> str:
        if isinstance(event_message, dict):
            self._register_subscription(event_message)
            return ""
        if not isinstance(event_message, list) or len(event_message) < 2:
            return ""
        entry = self._channels.get(int(event_message[0]))
        if entry is None or event_message[1] == CONSTANTS.WS_HEARTBEAT:
            return ""
        channel, _ = entry
        body = event_message[1]
        if channel == CONSTANTS.WS_BOOK_CHANNEL and isinstance(body, list):
            return self._snapshot_messages_queue_key if body and isinstance(body[0], list) else self._diff_messages_queue_key
        if channel == CONSTANTS.WS_TRADES_CHANNEL and body == "te":  # "tu" repeats "te" with the final id: ignore
            return self._trade_messages_queue_key
        return ""

    async def _parse_order_book_snapshot_message(self, raw_message, message_queue: asyncio.Queue):
        _, trading_pair = self._channels[int(raw_message[0])]
        bids, asks = self._split_levels(raw_message[1])
        message_queue.put_nowait(OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": trading_pair, "update_id": next(self._update_ids), "bids": bids, "asks": asks}, time.time()))

    async def _parse_order_book_diff_message(self, raw_message, message_queue: asyncio.Queue):
        _, trading_pair = self._channels[int(raw_message[0])]
        bids, asks = self._split_levels([raw_message[1]])
        message_queue.put_nowait(OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": trading_pair, "update_id": next(self._update_ids), "bids": bids, "asks": asks}, time.time()))

    async def _parse_trade_message(self, raw_message, message_queue: asyncio.Queue):
        _, trading_pair = self._channels[int(raw_message[0])]
        trade_id, mts, amount, price = raw_message[2][:4]
        message_queue.put_nowait(OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": trading_pair,
            "trade_type": float(TradeType.BUY.value if float(amount) > 0 else TradeType.SELL.value),
            "trade_id": str(trade_id), "update_id": next(self._update_ids),
            "price": float(price), "amount": abs(float(amount)),
        }, float(mts) / 1000))

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            channel = self._channel_originating_message(event_message=data)
            if channel:
                self._message_queue[channel].put_nowait(data)
