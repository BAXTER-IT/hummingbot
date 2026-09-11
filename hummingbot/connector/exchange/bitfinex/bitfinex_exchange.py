"""Bitfinex spot connector: public market data over REST/websocket, every order message over the
FIX 4.4 gateway (see ``fix/``), the few private reads over REST.

Slice B2 (this file's public surface): symbols, trading rules with Bitfinex's five-significant-digit
price rule, last price, platform status. The order path (B3) and the private reads (B4) raise
NotImplementedError until their slices land — the connector is constructible and usable for market
data with ``trading_required=False``.
"""

from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.exchange.bitfinex import (
    bitfinex_constants as CONSTANTS,
    bitfinex_utils as utils,
    bitfinex_web_utils as web_utils,
)
from hummingbot.connector.exchange.bitfinex.bitfinex_api_order_book_data_source import BitfinexAPIOrderBookDataSource
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class _NoAuth(AuthBase):
    async def rest_authenticate(self, request):
        return request

    async def ws_authenticate(self, request):
        return request


class BitfinexExchange(ExchangePyBase):
    web_utils = web_utils

    def __init__(
        self,
        bitfinex_api_key: str,
        bitfinex_secret_key: str,
        bitfinex_fix_username: str,
        bitfinex_fix_password: str,
        bitfinex_fix_sender_comp_id: str,
        bitfinex_fix_host: str = "",
        bitfinex_fix_port: int = 0,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self._api_key = bitfinex_api_key
        self._secret_key = bitfinex_secret_key
        self._fix_username = bitfinex_fix_username
        self._fix_password = bitfinex_fix_password
        self._fix_sender_comp_id = bitfinex_fix_sender_comp_id
        self._fix_host, self._fix_port = bitfinex_fix_host, int(bitfinex_fix_port or 0)
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # -- identity -----------------------------------------------------------------------
    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> AuthBase:
        return _NoAuth()  # B4 brings the REST HMAC auth for the private reads

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.PAIR_INFO_PATH

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.PAIR_LIST_PATH

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PLATFORM_STATUS_PATH

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False  # a FIX cancel is acknowledged by a later ExecutionReport

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    # -- public surface -----------------------------------------------------------------
    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any):
        """Accepts both ``pub:list:pair:exchange`` (``[["BTCUSD", "BTCUST", ...]]``) and ``pub:info:pair``
        (``[[["BTCUSD", [...]], ...]]``): the base class feeds it the trading-rules response too."""
        entries = exchange_info[0] if exchange_info and isinstance(exchange_info[0], list) else exchange_info
        mapping = bidict()
        for entry in entries:
            symbol = entry[0] if isinstance(entry, list) else entry
            if symbol.startswith("TEST"):
                continue
            try:
                mapping["t" + symbol] = utils.trading_pair_from_symbol(symbol)
            except ValueError:
                self.logger().debug(f"Skipping Bitfinex symbol {symbol}: cannot split into base and quote")
        self._set_trading_pair_symbol_map(mapping)

    async def _format_trading_rules(self, exchange_info: Any) -> List[TradingRule]:
        """``pub:info:pair`` rows: [symbol, [.., .., .., min_order_size, max_order_size, ...]]. Prices follow the
        five-significant-digit rule (see get_order_price_quantum), amounts have eight decimals."""
        rows = exchange_info[0] if exchange_info and isinstance(exchange_info[0], list) and exchange_info[0] \
            and isinstance(exchange_info[0][0], list) else exchange_info
        rules = []
        for symbol, info in rows:
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol="t" + symbol)
            except KeyError:
                continue
            rules.append(TradingRule(
                trading_pair=trading_pair,
                min_order_size=Decimal(str(info[CONSTANTS.PAIR_INFO_MIN_ORDER_SIZE])),
                max_order_size=Decimal(str(info[CONSTANTS.PAIR_INFO_MAX_ORDER_SIZE])),
                min_price_increment=utils.price_quantum(Decimal("1")),  # nominal; the real step depends on the price
                min_base_amount_increment=utils.AMOUNT_QUANTUM,
                min_quote_amount_increment=utils.AMOUNT_QUANTUM,
                min_notional_size=Decimal("0"),
            ))
        return rules

    def get_order_price_quantum(self, trading_pair: str, price: Decimal) -> Decimal:
        return utils.price_quantum(price)

    def get_order_size_quantum(self, trading_pair: str, order_size: Decimal) -> Decimal:
        return utils.AMOUNT_QUANTUM

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        ticker = await self._api_get(path_url=CONSTANTS.TICKER_PATH.format(symbol=symbol),
                                     limit_id=CONSTANTS.TICKER_PATH)
        return float(ticker[CONSTANTS.TICKER_LAST])

    async def check_network(self) -> NetworkStatus:
        try:
            status = await self._api_get(path_url=CONSTANTS.PLATFORM_STATUS_PATH, limit_id=CONSTANTS.PLATFORM_STATUS_PATH)
        except Exception:  # noqa: BLE001 — any failure to reach the venue is "not connected"
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED if status and status[0] == 1 else NetworkStatus.NOT_CONNECTED

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(throttler=self._throttler, auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return BitfinexAPIOrderBookDataSource(trading_pairs=self._trading_pairs, connector=self,
                                              api_factory=self._web_assistants_factory, domain=self._domain)

    # -- fees (configured schema; Bitfinex reports none on FIX) --------------------------
    def _get_fee(self, base_currency: str, quote_currency: str, order_type: OrderType, order_side: TradeType,
                 amount: Decimal, price: Decimal = Decimal("nan"), is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        is_maker = is_maker if is_maker is not None else (order_type is OrderType.LIMIT_MAKER)
        return build_trade_fee(self.name, is_maker, base_currency=base_currency, quote_currency=quote_currency,
                               order_type=order_type, order_side=order_side, amount=amount, price=price)

    async def _update_trading_fees(self):
        pass  # configured schema; B4 adds the optional REST summary cross-check

    # -- error classification ----------------------------------------------------------------
    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return "not found" in str(status_update_exception).lower()

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return "unknown order" in str(cancelation_exception).lower()

    # -- order path over FIX: slice B3 ----------------------------------------------------------
    async def _place_order(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                           order_type: OrderType, price: Decimal, **kwargs) -> Tuple[str, float]:
        raise NotImplementedError("Bitfinex orders go over FIX: slice B3")

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        raise NotImplementedError("Bitfinex cancels go over FIX: slice B3")

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return UserStreamTrackerDataSource()  # replaced by the FIX session in slice B3

    async def _user_stream_event_listener(self):
        raise NotImplementedError("slice B3")

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        raise NotImplementedError("slice B4")

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        raise NotImplementedError("slice B4")

    # -- balances: ABOS's published trade limits, slice B4 -------------------------------------
    async def _update_balances(self):
        raise NotImplementedError("slice B4: balances are ABOS's published trade limits")

    async def _api_get(self, path_url: str, limit_id: Optional[str] = None, **kwargs):
        return await self._api_request(path_url=path_url, method=RESTMethod.GET, limit_id=limit_id, **kwargs)
