"""Bitfinex spot connector: public market data over REST/websocket, every order message over the
FIX 4.4 gateway (see ``fix/``), the few private reads over REST.

Slice B2 (this file's public surface): symbols, trading rules with Bitfinex's five-significant-digit
price rule, last price, platform status. The order path (B3) and the private reads (B4) raise
NotImplementedError until their slices land — the connector is constructible and usable for market
data with ``trading_required=False``.
"""

import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.exchange.bitfinex import (
    bitfinex_constants as CONSTANTS,
    bitfinex_utils as utils,
    bitfinex_web_utils as web_utils,
)
from hummingbot.connector.exchange.bitfinex.bitfinex_api_order_book_data_source import BitfinexAPIOrderBookDataSource
from hummingbot.connector.exchange.bitfinex.bitfinex_fix_gateway import BitfinexFixGateway, FixOrderRejected
from hummingbot.connector.exchange.bitfinex.bitfinex_fix_user_stream_data_source import BitfinexFixUserStreamDataSource
from hummingbot.connector.exchange.bitfinex.bitfinex_trade_limits import (
    AbosTradeLimitsClient,
    TradeLimit,
    TradeLimitBudgetChecker,
)
from hummingbot.core.data_type.common import PriceType
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
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
        bitfinex_fix_tls: bool = True,
        bitfinex_abos_url: str = "",
        bitfinex_abos_username: str = "",
        bitfinex_abos_password: str = "",
        bitfinex_abos_account_id: int = 0,
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
        self._fix_gateway = BitfinexFixGateway(
            host=bitfinex_fix_host, port=int(bitfinex_fix_port or 0), sender_comp_id=bitfinex_fix_sender_comp_id,
            username=bitfinex_fix_username, password=bitfinex_fix_password, use_tls=bitfinex_fix_tls,
            log=lambda line: self.logger().info(line),
        )
        self._trade_limits: Dict[str, TradeLimit] = {}
        self._abos_client = AbosTradeLimitsClient(bitfinex_abos_url, bitfinex_abos_username, bitfinex_abos_password,
                                                  bitfinex_abos_account_id) if bitfinex_abos_url else None
        self._trade_limit_budget_checker: Optional[TradeLimitBudgetChecker] = None
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

    # -- order path over FIX -----------------------------------------------------------------
    async def _place_order(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                           order_type: OrderType, price: Decimal, **kwargs) -> Tuple[str, float]:
        try:
            return await self._fix_gateway.place_order(
                client_order_id=order_id, trading_pair=trading_pair, is_buy=trade_type is TradeType.BUY, amount=amount,
                price=None if order_type is OrderType.MARKET or price is None or price.is_nan() else price,
                order_type="market" if order_type is OrderType.MARKET else "limit",
                post_only=order_type is OrderType.LIMIT_MAKER,
            )
        except FixOrderRejected as err:
            raise IOError(f"Bitfinex rejected {order_id}: {err}") from err

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """Sends the cancel; the venue's ExecutionReport (or OrderCancelReject) finalises it later."""
        try:
            await self._fix_gateway.cancel_order(client_order_id=order_id, exchange_order_id=tracked_order.exchange_order_id)
        except FixOrderRejected as err:
            raise IOError(f"Bitfinex cancel of {order_id} not sent: {err}") from err
        return True

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BitfinexFixUserStreamDataSource(self._fix_gateway)

    def _is_user_stream_initialized(self) -> bool:
        return self._fix_gateway.logged_on or not self.is_trading_required

    async def _user_stream_event_listener(self):
        async for event in self._iter_user_event_queue():
            try:
                await self._process_fix_event(event)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — one bad report must not stop the stream
                self.logger().exception(f"Error processing FIX event {event.get('kind')} for {event.get('client_order_id')}")

    @staticmethod
    async def _await_if_needed(result):
        """The order tracker schedules updates as tasks; awaiting them keeps report order and state in step."""
        if asyncio.isfuture(result) or asyncio.iscoroutine(result):
            return await result
        return result

    _ORD_STATUS_TO_STATE = {"0": OrderState.OPEN, "1": OrderState.PARTIALLY_FILLED, "2": OrderState.FILLED,
                            "4": OrderState.CANCELED, "5": OrderState.OPEN, "8": OrderState.FAILED}

    async def _process_fix_event(self, event: Dict[str, Any]) -> None:
        kind = event["kind"]
        if kind == "execution_report":
            client_order_id = event["client_order_id"]
            fillable = self._order_tracker.all_fillable_orders.get(client_order_id)
            updatable = self._order_tracker.all_updatable_orders.get(client_order_id)
            if event["exec_type"] == "F" and fillable is not None and event["last_qty"]:
                is_maker = event["is_maker"] if event["is_maker"] is not None else fillable.order_type is OrderType.LIMIT_MAKER
                fee = self.get_fee(fillable.base_asset, fillable.quote_asset, fillable.order_type, fillable.trade_type,
                                   event["last_qty"], event["last_px"], is_maker=is_maker)
                await self._await_if_needed(self._order_tracker.process_trade_update(TradeUpdate(
                    trade_id=event["exec_id"], client_order_id=client_order_id,
                    exchange_order_id=event["exchange_order_id"] or fillable.exchange_order_id,
                    trading_pair=fillable.trading_pair, fill_timestamp=event["timestamp"], fill_price=event["last_px"],
                    fill_base_amount=event["last_qty"], fill_quote_amount=event["last_qty"] * event["last_px"], fee=fee,
                    is_taker=not is_maker,
                )))
            if updatable is not None:
                new_state = self._ORD_STATUS_TO_STATE.get(event["ord_status"], updatable.current_state)
                await self._await_if_needed(self._order_tracker.process_order_update(OrderUpdate(
                    trading_pair=updatable.trading_pair, update_timestamp=event["timestamp"], new_state=new_state,
                    client_order_id=client_order_id, exchange_order_id=event["exchange_order_id"] or updatable.exchange_order_id,
                    misc_updates={"text": event["text"]} if event["text"] else None,
                )))
        elif kind == "cancel_reject":
            if event["reason"] == "1":  # unknown order: the venue does not have it
                await self._order_tracker.process_order_not_found(event["client_order_id"])
            else:
                self.logger().info(f"Bitfinex cancel reject for {event['client_order_id']}: {event['text']} (reason {event['reason']})")
        elif kind == "disconnected":
            # Session-TIF orders die with the FIX connection: the gateway cancels them on disconnect.
            for order in list(self._order_tracker.all_updatable_orders.values()):
                await self._await_if_needed(self._order_tracker.process_order_update(OrderUpdate(
                    trading_pair=order.trading_pair, update_timestamp=event["timestamp"], new_state=OrderState.CANCELED,
                    client_order_id=order.client_order_id, exchange_order_id=order.exchange_order_id,
                    misc_updates={"text": f"FIX session ended: {event['text']}"},
                )))
        elif kind == "pre_close":
            self.logger().warning(f"Bitfinex session pre-close: {event['text']} — no new orders until the next logon")

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        raise NotImplementedError("slice B4")

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        raise NotImplementedError("slice B4")

    # -- balances: ABOS's published trade limits ------------------------------------------------
    @property
    def budget_checker(self) -> TradeLimitBudgetChecker:
        if self._trade_limit_budget_checker is None:
            self._trade_limit_budget_checker = TradeLimitBudgetChecker(self, lambda: self._trade_limits)
        return self._trade_limit_budget_checker

    @property
    def trade_limits(self) -> Dict[str, TradeLimit]:
        return dict(self._trade_limits)

    async def _update_balances(self):
        if self._abos_client is None:
            raise RuntimeError("Bitfinex balances are ABOS's published trade limits: configure bitfinex_abos_url/"
                               "username/password/account_id")
        limits = await self._abos_client.fetch()
        mids: Dict[str, Decimal] = {}
        for pair in limits:
            try:
                mid = self.get_price_by_type(pair, PriceType.MidPrice)
                mids[pair] = mid if mid and not mid.is_nan() else Decimal("0")
            except Exception:  # noqa: BLE001 — no book for this pair: quote allowance unpriced
                mids[pair] = Decimal("0")
        self._apply_trade_limits(limits, mids)

    def _apply_trade_limits(self, limits: Dict[str, TradeLimit], mid_prices: Dict[str, Decimal]) -> None:
        """Balances derived from the allowance: base = what we may still sell, quote = what we may still buy,
        priced at mid. The budget checker uses the allowance directly; these are for inventory logic and display."""
        self._trade_limits = dict(limits)
        base_totals: Dict[str, Decimal] = {}
        quote_totals: Dict[str, Decimal] = {}
        for pair, limit in limits.items():
            base, quote = pair.split("-")
            base_totals[base] = base_totals.get(base, Decimal("0")) + limit.max_sell
            quote_totals[quote] = quote_totals.get(quote, Decimal("0")) + limit.max_buy * mid_prices.get(pair, Decimal("0"))
        balances = {**quote_totals}
        for asset, amount in base_totals.items():
            balances[asset] = balances.get(asset, Decimal("0")) + amount
        self._account_balances = dict(balances)
        self._account_available_balances = dict(balances)

    async def _api_get(self, path_url: str, limit_id: Optional[str] = None, **kwargs):
        return await self._api_request(path_url=path_url, method=RESTMethod.GET, limit_id=limit_id, **kwargs)
