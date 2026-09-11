"""The order gateway: Hummingbot orders in, Bitfinex execution reports out, over one FIX session.

Owns the session's lifecycle (connect, reconnect with backoff, the daily session end), the
mapping between Hummingbot's client order id and the chain of FIX ClOrdIDs a single order goes
through (the original request, then each cancel request — Bitfinex reflects the *latest* request
id on every report), and the correlation of a NewOrderSingle with its first ExecutionReport so
``place_order`` can answer "accepted with venue id X" or "rejected because Y".

Every report is also pushed onto ``events`` as a plain dict for the connector's user-stream
listener; the gateway never touches Hummingbot's order tracker itself.
"""

from __future__ import annotations

import asyncio
import itertools
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Dict, Optional, Tuple

from hummingbot.connector.exchange.bitfinex import bitfinex_constants as CONSTANTS, bitfinex_utils as utils
from hummingbot.connector.exchange.bitfinex.fix.message import FixMessage, FixProtocolError
from hummingbot.connector.exchange.bitfinex.fix.session import FixLogonError, FixSession, SessionState

Log = Callable[[str], None]

MSG_EXECUTION_REPORT, MSG_CANCEL_REJECT, MSG_TRADING_SESSION_STATUS = "8", "9", "h"
EXEC_NEW, EXEC_CANCELED, EXEC_REPLACED, EXEC_REJECTED, EXEC_TRADE = "0", "4", "5", "8", "F"


class FixOrderRejected(RuntimeError):
    """The venue refused the order (or the session cannot take orders right now); text is the venue's."""


def _parse_fix_time(text: str) -> float:
    try:
        return datetime.strptime(text, "%Y%m%d-%H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp()
    except (ValueError, TypeError):
        return time.time()


def _dec(value: Optional[str]) -> Optional[Decimal]:
    return Decimal(value) if value not in (None, "") else None


class BitfinexFixGateway:
    def __init__(
        self,
        host: str,
        port: int,
        sender_comp_id: str,
        username: str,
        password: str,
        target_comp_id: str = CONSTANTS.FIX_TARGET_COMP_ID,
        heartbeat_s: float = CONSTANTS.FIX_DEFAULT_HEARTBEAT_S,
        use_tls: bool = True,
        seq_store: Optional[Dict[str, int]] = None,
        reconnect_wait_s: float = 2.0,
        max_reconnect_wait_s: float = 30.0,
        place_timeout_s: float = 10.0,
        log: Log = print,
    ) -> None:
        self._session_kwargs = dict(host=host, port=port, sender_comp_id=sender_comp_id, target_comp_id=target_comp_id,
                                    username=username, password=password, heartbeat_s=heartbeat_s, use_tls=use_tls,
                                    seq_store=seq_store if seq_store is not None else {}, log=log)
        self._reconnect_wait_s, self._max_reconnect_wait_s = reconnect_wait_s, max_reconnect_wait_s
        self._place_timeout_s = place_timeout_s
        self._log = log
        self.events: asyncio.Queue = asyncio.Queue()
        self.pre_close = False
        self.last_recv_time = 0.0
        self._session: Optional[FixSession] = None
        self._run_task: Optional[asyncio.Task] = None
        self._running = False
        self._logged_on = asyncio.Event()
        self._disconnected = asyncio.Event()
        self._pending: Dict[str, asyncio.Future] = {}          # ClOrdID of a NewOrderSingle -> first report
        self._latest_cl_ord_id: Dict[str, str] = {}           # client order id -> latest accepted request id
        self._client_id_for: Dict[str, str] = {}              # any request id -> client order id
        self._cancel_counter = itertools.count(1)

    # -- lifecycle ----------------------------------------------------------------------
    @property
    def logged_on(self) -> bool:
        return self._session is not None and self._session.logged_on

    async def start(self) -> None:
        self._running = True
        self._run_task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        if self._run_task:
            self._run_task.cancel()
            try:
                await self._run_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        if self._session is not None:
            await self._session.close()

    async def wait_logged_on(self, timeout: float = 30.0) -> None:
        await asyncio.wait_for(self._logged_on.wait(), timeout)

    async def _run(self) -> None:
        wait = self._reconnect_wait_s
        while self._running:
            self._disconnected.clear()
            self._session = FixSession(on_app_message=self._on_message, on_state=self._on_state, **self._session_kwargs)
            try:
                await self._session.connect()
                wait = self._reconnect_wait_s
                self.pre_close = False
                self._logged_on.set()
                await self._disconnected.wait()
            except FixLogonError as err:
                self._log(f"fix gateway: logon failed: {err}")
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 — never let the reconnect loop die
                self._log(f"fix gateway: session error: {type(err).__name__}: {err}")
            self._logged_on.clear()
            self._fail_pending("session lost")
            if not self._running:
                return
            await asyncio.sleep(wait)
            wait = min(wait * 2, self._max_reconnect_wait_s)

    def _on_state(self, state: SessionState, text: str) -> None:
        if state in (SessionState.DISCONNECTED, SessionState.LOGGED_OUT):
            if state is SessionState.DISCONNECTED:
                self._logged_on.clear()  # before anyone can observe the event: "logged on" must mean the new session
                self.events.put_nowait({"kind": "disconnected", "text": text or "connection closed", "timestamp": time.time()})
                self._disconnected.set()

    def _fail_pending(self, why: str) -> None:
        for cl_ord_id, future in list(self._pending.items()):
            if not future.done():
                future.set_exception(FixOrderRejected(f"{why} before the venue answered {cl_ord_id}"))
        self._pending.clear()

    # -- orders -------------------------------------------------------------------------
    async def place_order(self, client_order_id: str, trading_pair: str, is_buy: bool, amount: Decimal,
                          price: Optional[Decimal], order_type: str, post_only: bool) -> Tuple[str, float]:
        """Send a NewOrderSingle and wait for the venue's first report. Returns (OrderID, transact time)."""
        if not self.logged_on:
            raise FixOrderRejected("FIX session is not logged on")
        if self.pre_close:
            raise FixOrderRejected("Bitfinex session is in pre-close (daily session ending); no new orders")
        if len(client_order_id) > CONSTANTS.FIX_CLIENT_ORDER_ID_MAX_LENGTH:
            raise FixOrderRejected(f"client order id longer than {CONSTANTS.FIX_CLIENT_ORDER_ID_MAX_LENGTH}")
        body = [("11", client_order_id), ("55", utils.fix_symbol(trading_pair)), ("54", "1" if is_buy else "2"),
                ("38", f"{amount.normalize():f}"), ("40", "1" if order_type == "market" else "2")]
        if order_type != "market":
            if price is not None:
                body.append(("44", f"{price.normalize():f}"))
            body.append(("59", CONSTANTS.FIX_TIF_SESSION))
            if post_only:
                body.append(("3927", str(CONSTANTS.FIX_ORDER_FLAG_POST_ONLY)))
        body.append(("6061", "N"))
        future = asyncio.get_running_loop().create_future()
        self._pending[client_order_id] = future
        self._latest_cl_ord_id[client_order_id] = client_order_id
        self._client_id_for[client_order_id] = client_order_id
        try:
            await self._session.send(FixMessage("D", body))
            report = await asyncio.wait_for(future, self._place_timeout_s)
        except asyncio.TimeoutError:
            raise FixOrderRejected(f"no execution report for {client_order_id} within {self._place_timeout_s}s") from None
        except FixProtocolError as err:
            raise FixOrderRejected(str(err)) from None
        finally:
            self._pending.pop(client_order_id, None)
        if report["exec_type"] == EXEC_REJECTED:
            raise FixOrderRejected(report["text"] or "rejected by the venue")
        return report["exchange_order_id"], report["timestamp"]

    async def cancel_order(self, client_order_id: str, exchange_order_id: Optional[str]) -> str:
        """Send an OrderCancelRequest for the order's latest request id; returns the cancel's own ClOrdID."""
        if not self.logged_on:
            raise FixOrderRejected("FIX session is not logged on")
        orig = self._latest_cl_ord_id.get(client_order_id, client_order_id)
        cancel_id = f"{client_order_id[:30]}-c{next(self._cancel_counter):03d}"[:CONSTANTS.FIX_CLIENT_ORDER_ID_MAX_LENGTH]
        self._client_id_for[cancel_id] = client_order_id
        body = [("11", cancel_id), ("41", orig)]
        if exchange_order_id and exchange_order_id != "0":
            body.insert(0, ("37", exchange_order_id))
        await self._session.send(FixMessage("F", body))
        return cancel_id

    # -- inbound ------------------------------------------------------------------------
    def _on_message(self, msg: FixMessage) -> None:
        self.last_recv_time = time.time()
        if msg.msg_type == MSG_EXECUTION_REPORT:
            self._on_execution_report(msg)
        elif msg.msg_type == MSG_CANCEL_REJECT:
            request_id = msg.get("11", "") or ""
            self.events.put_nowait({
                "kind": "cancel_reject", "client_order_id": self._client_id_for.get(request_id, request_id),
                "request_id": request_id, "orig_request_id": msg.get("41"), "exchange_order_id": msg.get("37"),
                "ord_status": msg.get("39"), "reason": msg.get("102"), "text": msg.get("58", "") or "",
                "timestamp": time.time(),
            })
        elif msg.msg_type == MSG_TRADING_SESSION_STATUS:
            if msg.get("340") == "5":
                self.pre_close = True
                self.events.put_nowait({"kind": "pre_close", "text": msg.get("58", "") or "", "timestamp": time.time()})
        else:
            self._log(f"fix gateway: unhandled {msg.msg_type} #{msg.seq_num}")

    def _on_execution_report(self, msg: FixMessage) -> None:
        request_id = msg.get("11", "") or ""
        client_order_id = self._client_id_for.get(request_id, request_id)
        exec_type = msg.get("150", "")
        if exec_type in (EXEC_NEW, EXEC_REPLACED) or (exec_type == EXEC_CANCELED and request_id != client_order_id):
            self._latest_cl_ord_id[client_order_id] = request_id
        event = {
            "kind": "execution_report", "client_order_id": client_order_id, "request_id": request_id,
            "exchange_order_id": msg.get("37"), "exec_id": msg.get("17"), "exec_type": exec_type,
            "ord_status": msg.get("39"), "symbol": msg.get("55"), "side": msg.get("54"),
            "last_qty": _dec(msg.get("32")), "last_px": _dec(msg.get("31")), "order_qty": _dec(msg.get("38")),
            "leaves_qty": _dec(msg.get("151")), "cum_qty": _dec(msg.get("14")), "avg_px": _dec(msg.get("6")),
            "is_maker": msg.get("1057") == "N" if msg.get("1057") else None,
            "text": msg.get("58", "") or "", "timestamp": _parse_fix_time(msg.get("60", "")), "poss_dup": msg.poss_dup,
        }
        future = self._pending.get(request_id)
        if future is not None and not future.done():
            future.set_result(event)
        self.events.put_nowait(event)
