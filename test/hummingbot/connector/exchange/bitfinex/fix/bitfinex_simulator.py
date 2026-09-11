"""A stand-in for the Bitfinex FIX 4.4 gateway, faithful to the v1.21 spec in the ways that bite.

Logon with Username/Password (a wrong one gets a Logout and a dropped connection); heartbeats and
TestRequest; sequence numbers with ResendRequest / SequenceReset-GapFill / PossDup replay;
NewOrderSingle → ExecutionReport(New) and, when the test asks, a fill; OrderCancelRequest by
OrigClOrdID → ExecutionReport(Canceled) or OrderCancelReject(unknown order); TradingSessionStatus
Pre-Close + Logout for the daily session end. Tests drive it through the public methods.
"""

import asyncio
import itertools
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from hummingbot.connector.exchange.bitfinex.fix.message import FixMessage, FixParser, encode


def now_fix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]


class BitfinexFixSimulator:
    def __init__(self, username="user", password="pw", comp_id="BfxComp", heartbeat_s: Optional[float] = None):
        self.username, self.password, self.comp_id = username, password, comp_id
        self.heartbeat_s = heartbeat_s  # None = accept the client's
        self.received: List[FixMessage] = []          # every message from the client, in order
        self.sent: List[bytes] = []                    # every frame we sent (raw), for PossDup replay
        self.sent_msgs: List[Tuple[int, FixMessage]] = []
        self.orders: Dict[str, dict] = {}              # last accepted ClOrdID -> {order_id, symbol, side, qty, price, cum}
        self.fill_next = False                         # fill the next accepted order immediately
        self.skip_next_seq = False                     # leave a gap in our numbering (test the client's resend)
        self.logons = 0
        self.connections = 0
        self._order_ids = itertools.count(1000)
        self._writer: Optional[asyncio.StreamWriter] = None
        self._out_seq = 1
        self._in_seq_expected = 1
        self._client_comp = ""
        self._server: Optional[asyncio.AbstractServer] = None
        self._logged_on = asyncio.Event()
        self._closed = asyncio.Event()
        self._client_seen = asyncio.Condition()

    # -- lifecycle --------------------------------------------------------------------
    async def start(self) -> Tuple[str, int]:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        host, port = self._server.sockets[0].getsockname()[:2]
        return host, port

    async def stop(self):
        if self._writer:
            self._writer.close()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def wait_logged_on(self, timeout=5):
        await asyncio.wait_for(self._logged_on.wait(), timeout)

    async def wait_for(self, predicate, timeout=5):
        """Wait until predicate(received) is true."""
        async with self._client_seen:
            await asyncio.wait_for(self._client_seen.wait_for(lambda: predicate(self.received)), timeout)

    async def drop_connection(self):
        if self._writer:
            self._writer.close()
            self._writer = None

    def reset_for_new_day(self):
        self._out_seq = 1
        self._in_seq_expected = 1
        self.sent.clear()
        self.sent_msgs.clear()

    # -- outbound (test-driven) ---------------------------------------------------------
    async def send_test_request(self, req_id="tr1"):
        await self._send(FixMessage("1", [("112", req_id)]))

    async def send_logout(self, text="Trading session ending"):
        await self._send(FixMessage("5", [("58", text)]))

    async def send_pre_close(self):
        await self._send(FixMessage("h", [("336", datetime.now(timezone.utc).strftime("%y%m%d")), ("340", "5"),
                                          ("58", "Trading session ending")]))

    async def send_resend_request(self, begin: int, end: int = 0):
        await self._send(FixMessage("2", [("7", str(begin)), ("16", str(end))]))

    async def fill(self, cl_ord_id: str, qty: Optional[str] = None, price: Optional[str] = None, aggressor="N"):
        order = self.orders[cl_ord_id]
        qty = qty or order["qty"]
        price = price or order["price"] or "100"
        order["cum"] = str(float(order["cum"]) + float(qty))
        leaves = float(order["qty"]) - float(order["cum"])
        await self._send(FixMessage("8", [
            ("37", order["order_id"]), ("11", cl_ord_id), ("17", f"exec-{next(self._order_ids)}"), ("150", "F"),
            ("39", "2" if leaves <= 0 else "1"), ("40", order["ord_type"]), ("55", order["symbol"]), ("54", order["side"]),
            ("44", order["price"] or ""), ("32", qty), ("31", price), ("38", order["qty"]), ("151", f"{max(leaves, 0):g}"),
            ("14", order["cum"]), ("6", price), ("60", now_fix()), ("1057", aggressor),
        ]))

    # -- the wire ---------------------------------------------------------------------
    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.connections += 1
        self._writer = writer
        parser = FixParser()
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                for msg in parser.feed(data):
                    await self._on_message(msg)
        except (ConnectionError, asyncio.IncompleteReadError, asyncio.CancelledError):
            pass
        finally:
            self._logged_on.clear()
            if self._writer is writer:
                self._writer = None
            writer.close()

    async def _on_message(self, msg: FixMessage):
        self.received.append(msg)
        async with self._client_seen:
            self._client_seen.notify_all()
        if msg.msg_type == "A":
            return await self._on_logon(msg)
        if not self._logged_on.is_set():
            return
        if msg.seq_num > self._in_seq_expected:
            await self._send(FixMessage("2", [("7", str(self._in_seq_expected)), ("16", "0")]))
            return
        if msg.seq_num < self._in_seq_expected and not msg.poss_dup and msg.msg_type != "4":
            await self.send_logout(f"MsgSeqNum too low, expecting {self._in_seq_expected} but received {msg.seq_num}")
            await self.drop_connection()
            return
        if msg.msg_type == "4":  # SequenceReset / GapFill
            self._in_seq_expected = int(msg.get("36"))
            return
        self._in_seq_expected = msg.seq_num + 1
        if msg.msg_type == "0":
            return
        if msg.msg_type == "1":
            return await self._send(FixMessage("0", [("112", msg.get("112"))]))
        if msg.msg_type == "2":
            return await self._resend(int(msg.get("7")), int(msg.get("16")))
        if msg.msg_type == "5":
            await self._send(FixMessage("5"))
            return await self.drop_connection()
        if msg.msg_type == "D":
            return await self._on_new_order(msg)
        if msg.msg_type == "F":
            return await self._on_cancel(msg)

    async def _on_logon(self, msg: FixMessage):
        self._client_comp = msg.sender
        if msg.get("553") != self.username or msg.get("554") != self.password:
            await self._send(FixMessage("5", [("58", "Invalid credentials")]))
            return await self.drop_connection()
        if msg.get("141") == "Y":
            self._out_seq = 1
            self._in_seq_expected = 1
            self.sent.clear()
            self.sent_msgs.clear()
        self._in_seq_expected = msg.seq_num + 1
        self.logons += 1
        hb = str(self.heartbeat_s or msg.get("108"))
        self._logged_on.set()
        await self._send(FixMessage("A", [("98", "0"), ("108", hb)] + ([("141", "Y")] if msg.get("141") == "Y" else [])))

    async def _on_new_order(self, msg: FixMessage):
        cl = msg.get("11")
        if msg.get("40") == "2" and not msg.get("44"):
            return await self._send(FixMessage("8", [("37", "0"), ("11", cl), ("17", "rej"), ("150", "8"), ("39", "8"),
                                                     ("40", msg.get("40")), ("55", msg.get("55")), ("54", msg.get("54")),
                                                     ("38", msg.get("38")), ("151", "0"), ("14", "0"), ("6", "0"),
                                                     ("58", "Price required for limit order")]))
        order_id = str(next(self._order_ids))
        self.orders[cl] = {"order_id": order_id, "symbol": msg.get("55"), "side": msg.get("54"), "qty": msg.get("38"),
                           "price": msg.get("44"), "ord_type": msg.get("40"), "cum": "0", "flags": msg.get("3927")}
        await self._send(FixMessage("8", [("37", order_id), ("11", cl), ("17", f"exec-{order_id}"), ("150", "0"), ("39", "0"),
                                          ("40", msg.get("40")), ("55", msg.get("55")), ("54", msg.get("54")),
                                          ("44", msg.get("44") or ""), ("38", msg.get("38")), ("151", msg.get("38")),
                                          ("14", "0"), ("6", "0"), ("60", now_fix()), ("3927", msg.get("3927") or "0")]))
        if self.fill_next:
            self.fill_next = False
            await self.fill(cl)

    async def _on_cancel(self, msg: FixMessage):
        orig = msg.get("41")
        order = self.orders.get(orig)
        if order is None or float(order["qty"]) - float(order["cum"]) <= 0:
            return await self._send(FixMessage("9", [("37", order["order_id"] if order else "0"), ("11", msg.get("11")),
                                                     ("41", orig), ("39", "2" if order else "8"), ("434", "1"),
                                                     ("102", "0" if order else "1"),
                                                     ("58", "Too late to cancel" if order else "Unknown order")]))
        self.orders[msg.get("11")] = dict(order)  # the cancel's ClOrdID becomes the order's latest id
        await self._send(FixMessage("8", [("37", order["order_id"]), ("11", msg.get("11")), ("41", orig),
                                          ("17", f"exec-c-{order['order_id']}"), ("150", "4"), ("39", "4"),
                                          ("40", order["ord_type"]), ("55", order["symbol"]), ("54", order["side"]),
                                          ("44", order["price"] or ""), ("38", order["qty"]), ("151", "0"),
                                          ("14", order["cum"]), ("6", "0"), ("60", now_fix())]))
        order["cum"] = order["qty"]

    async def _resend(self, begin: int, end: int):
        """Replay application messages with PossDup; admin messages and never-sent numbers become one GapFill."""
        last = self._out_seq - 1
        end = last if end == 0 else min(end, last)
        by_seq = dict(self.sent_msgs)
        gap_from = None
        for seq in range(begin, end + 1):
            msg = by_seq.get(seq)
            if msg is None or msg.msg_type in ("0", "1", "2", "4", "5", "A"):
                gap_from = seq if gap_from is None else gap_from
                continue
            if gap_from is not None:
                await self._send_raw(FixMessage("4", [("123", "Y"), ("36", str(seq))]), seq=gap_from, poss_dup=True)
                gap_from = None
            await self._send_raw(msg, seq=seq, poss_dup=True)
        if gap_from is not None:
            await self._send_raw(FixMessage("4", [("123", "Y"), ("36", str(end + 1))]), seq=gap_from, poss_dup=True)

    async def _send(self, msg: FixMessage):
        if self.skip_next_seq:
            self.skip_next_seq = False
            self._out_seq += 1
        seq = self._out_seq
        self._out_seq += 1
        self.sent_msgs.append((seq, msg))
        await self._send_raw(msg, seq=seq)

    async def _send_raw(self, msg: FixMessage, seq: int, poss_dup=False):
        if self._writer is None:
            return
        raw = encode(msg, sender=self.comp_id, target=self._client_comp, seq_num=seq, sending_time=now_fix(),
                     poss_dup=poss_dup, orig_sending_time=now_fix() if poss_dup else "")
        self.sent.append(raw)
        self._writer.write(raw)
        try:
            await self._writer.drain()
        except ConnectionError:
            pass
