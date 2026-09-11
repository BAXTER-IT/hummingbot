"""An asyncio FIX 4.4 session for the Bitfinex order gateway (spec v1.21).

Owns the TCP/TLS connection, the logon handshake (Username/Password), heartbeats and test
requests, sequence numbers in both directions with gap recovery (we send ResendRequest on a
gap; we answer theirs with a SequenceReset-GapFill, since execution reports only ever flow
towards us), and the logout/disconnect bookkeeping. Application messages (D, F, G in; 8, 9, h
out) are handed to a callback untouched. Bitfinex ends every session at 00:00 UTC and resets
sequence numbers then; the sequence store keeps today's counters so a reconnect inside the day
continues numbering instead of resetting (which would make the gateway replay nothing).
"""

from __future__ import annotations

import asyncio
import ssl
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Awaitable, Callable, Dict, Optional, Union

from .message import FixMessage, FixParser, FixProtocolError, encode

Log = Callable[[str], None]
AppCallback = Callable[[FixMessage], Union[None, Awaitable[None]]]

MSG_HEARTBEAT, MSG_TEST_REQUEST, MSG_RESEND_REQUEST, MSG_REJECT, MSG_SEQUENCE_RESET, MSG_LOGOUT, MSG_LOGON = \
    "0", "1", "2", "3", "4", "5", "A"
ADMIN_TYPES = {MSG_HEARTBEAT, MSG_TEST_REQUEST, MSG_RESEND_REQUEST, MSG_REJECT, MSG_SEQUENCE_RESET, MSG_LOGOUT, MSG_LOGON}


class SessionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    LOGON_SENT = "logon_sent"
    LOGGED_ON = "logged_on"
    LOGOUT_SENT = "logout_sent"
    LOGGED_OUT = "logged_out"


class FixLogonError(RuntimeError):
    """The gateway refused or dropped the logon; the message carries its text, never a credential."""


def fix_time(ts: Optional[float] = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else time.time(), timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]


class FixSession:
    def __init__(
        self,
        host: str,
        port: int,
        sender_comp_id: str,
        target_comp_id: str,
        username: str,
        password: str,
        heartbeat_s: float = 10.0,
        use_tls: bool = True,
        ssl_context: Optional[ssl.SSLContext] = None,
        seq_store: Optional[Dict[str, int]] = None,
        reset_seq_num_on_logon: bool = False,
        protect_self_match: bool = False,
        on_app_message: Optional[AppCallback] = None,
        on_state: Optional[Callable[[SessionState, str], None]] = None,
        logon_timeout_s: float = 10.0,
        clock: Callable[[], float] = time.time,
        log: Log = print,
    ) -> None:
        self._host, self._port = host, port
        self._sender, self._target = sender_comp_id, target_comp_id
        self._username, self._password = username, password
        self._heartbeat_s = heartbeat_s
        self._use_tls, self._ssl_context = use_tls, ssl_context
        self._store = seq_store if seq_store is not None else {}
        self._reset_on_logon = reset_seq_num_on_logon
        self._protect_self_match = protect_self_match
        self._on_app = on_app_message
        self._on_state = on_state
        self._logon_timeout_s = logon_timeout_s
        self._clock = clock
        self._log = log
        self.state = SessionState.DISCONNECTED
        self.last_logout_text = ""
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._tasks: list = []
        self._send_lock = asyncio.Lock()
        self._logon_done: Optional[asyncio.Future] = None
        self._last_sent = 0.0
        self._last_received = 0.0
        self._pending_test_request: Optional[str] = None
        self._sent_log: Dict[int, FixMessage] = {}

    # -- sequence numbers (persisted per UTC day) -------------------------------------
    def _today(self) -> str:
        return datetime.fromtimestamp(self._clock(), timezone.utc).strftime("%Y%m%d")

    def _load_counters(self) -> None:
        if self._store.get("day") != self._today() or self._reset_on_logon:
            self._store.update(day=self._today(), out=1, expected_in=1)
        self._store.setdefault("out", 1)
        self._store.setdefault("expected_in", 1)

    @property
    def next_out_seq(self) -> int:
        return self._store["out"]

    @property
    def expected_in_seq(self) -> int:
        return self._store["expected_in"]

    # -- lifecycle ----------------------------------------------------------------------
    @property
    def logged_on(self) -> bool:
        return self.state is SessionState.LOGGED_ON

    async def connect(self) -> None:
        """Open the connection and complete the logon handshake, or raise FixLogonError."""
        self._load_counters()
        self._set_state(SessionState.CONNECTING)
        ctx = None
        if self._use_tls:
            ctx = self._ssl_context or ssl.create_default_context()
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port, ssl=ctx), self._logon_timeout_s)
        except (OSError, asyncio.TimeoutError) as err:
            self._set_state(SessionState.DISCONNECTED, f"connect failed: {err}")
            raise FixLogonError(f"cannot connect to {self._host}:{self._port}: {err}") from None
        self._last_received = self._last_sent = self._clock()
        self._logon_done = asyncio.get_running_loop().create_future()
        self._tasks = [asyncio.create_task(self._read_loop()), asyncio.create_task(self._timer_loop())]
        body = [("98", "0"), ("108", str(int(self._heartbeat_s)))]
        if self._reset_on_logon:
            body.append(("141", "Y"))
        body += [("553", self._username), ("554", self._password)]
        if self._protect_self_match:
            body.append(("6065", "Y"))
        self._set_state(SessionState.LOGON_SENT)
        await self._send(FixMessage(MSG_LOGON, body))
        try:
            await asyncio.wait_for(self._logon_done, self._logon_timeout_s)
        except asyncio.TimeoutError:
            await self._teardown("logon timed out")
            raise FixLogonError("no Logon acknowledgement from the gateway") from None
        if self.state is not SessionState.LOGGED_ON:
            text = self.last_logout_text or "connection dropped during logon"
            raise FixLogonError(f"logon refused: {text}")

    async def send(self, msg: FixMessage) -> int:
        """Send an application message; returns the sequence number it went out with."""
        if self.state is not SessionState.LOGGED_ON:
            raise FixProtocolError(f"cannot send {msg.msg_type}: session is {self.state.value}")
        return await self._send(msg)

    async def logout(self, text: str = "") -> None:
        if self.state is SessionState.LOGGED_ON:
            self._set_state(SessionState.LOGOUT_SENT)
            await self._send(FixMessage(MSG_LOGOUT, [("58", text)] if text else []))
            for _ in range(20):
                if self.state is SessionState.DISCONNECTED:
                    return
                await asyncio.sleep(0.1)
        await self._teardown("logout")

    async def close(self) -> None:
        await self._teardown("closed")

    # -- wire ----------------------------------------------------------------------------
    async def _send(self, msg: FixMessage, seq: Optional[int] = None, poss_dup: bool = False) -> int:
        async with self._send_lock:
            if seq is None:
                seq = self._store["out"]
                self._store["out"] = seq + 1
                if msg.msg_type not in ADMIN_TYPES:
                    self._sent_log[seq] = msg
            raw = encode(msg, sender=self._sender, target=self._target, seq_num=seq, sending_time=fix_time(self._clock()),
                         poss_dup=poss_dup, orig_sending_time=fix_time(self._clock()) if poss_dup else "")
            if self._writer is None:
                raise FixProtocolError("not connected")
            self._writer.write(raw)
            await self._writer.drain()
            self._last_sent = self._clock()
            return seq

    async def _read_loop(self) -> None:
        parser = FixParser()
        try:
            while self._reader is not None:
                data = await self._reader.read(65536)
                if not data:
                    break
                self._last_received = self._clock()
                for msg in parser.feed(data):
                    await self._on_message(msg)
        except (ConnectionError, asyncio.IncompleteReadError, ssl.SSLError) as err:
            self._log(f"fix: connection error: {err}")
        except FixProtocolError as err:
            self._log(f"fix: protocol error: {err}")
        except asyncio.CancelledError:
            return
        finally:
            if self.state is not SessionState.DISCONNECTED:
                asyncio.get_running_loop().call_soon(lambda: asyncio.ensure_future(self._teardown("connection closed")))

    async def _timer_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(max(self._heartbeat_s / 4, 0.05))
                if self.state not in (SessionState.LOGGED_ON, SessionState.LOGOUT_SENT):
                    continue
                now = self._clock()
                if now - self._last_sent >= self._heartbeat_s:
                    await self._send(FixMessage(MSG_HEARTBEAT))
                silent = now - self._last_received
                if self._pending_test_request is not None and silent >= 2 * self._heartbeat_s:
                    await self._teardown("no response to TestRequest")
                    return
                if self._pending_test_request is None and silent >= self._heartbeat_s * 1.2:
                    self._pending_test_request = f"tr{int(now * 1000)}"
                    await self._send(FixMessage(MSG_TEST_REQUEST, [("112", self._pending_test_request)]))
        except asyncio.CancelledError:
            return

    async def _on_message(self, msg: FixMessage) -> None:
        if msg.msg_type == MSG_LOGON and self.state is SessionState.LOGON_SENT:
            self._store["expected_in"] = msg.seq_num + 1
            self._set_state(SessionState.LOGGED_ON)
            if self._logon_done and not self._logon_done.done():
                self._logon_done.set_result(True)
            return
        if msg.msg_type == MSG_LOGOUT and self.state is SessionState.LOGON_SENT:
            self.last_logout_text = msg.get("58", "") or ""
            await self._teardown(f"logout during logon: {self.last_logout_text}", logged_out=True)
            return
        # sequence checks
        expected = self._store["expected_in"]
        if msg.msg_type == MSG_SEQUENCE_RESET and msg.get("123") == "Y":
            new_seq = int(msg.get("36", "0"))
            if new_seq >= expected:
                self._store["expected_in"] = new_seq
            return
        if msg.seq_num > expected:
            self._log(f"fix: gap: expected {expected}, got {msg.seq_num}; requesting resend")
            await self._send(FixMessage(MSG_RESEND_REQUEST, [("7", str(expected)), ("16", "0")]))
            return  # the gateway replays from `expected`, including this one
        if msg.seq_num < expected:
            if msg.poss_dup:
                return  # already processed
            await self._teardown(f"MsgSeqNum too low: {msg.seq_num} < {expected}")
            return
        self._store["expected_in"] = msg.seq_num + 1
        if msg.poss_dup:
            self._log(f"fix: replayed {msg.msg_type} #{msg.seq_num}")
        # admin
        if msg.msg_type == MSG_HEARTBEAT:
            if msg.get("112") == self._pending_test_request:
                self._pending_test_request = None
            return
        if msg.msg_type == MSG_TEST_REQUEST:
            await self._send(FixMessage(MSG_HEARTBEAT, [("112", msg.get("112", ""))]))
            return
        if msg.msg_type == MSG_RESEND_REQUEST:
            await self._answer_resend(int(msg.get("7", "1")), int(msg.get("16", "0")))
            return
        if msg.msg_type == MSG_LOGOUT:
            self.last_logout_text = msg.get("58", "") or ""
            if self.state is SessionState.LOGGED_ON:
                try:
                    await self._send(FixMessage(MSG_LOGOUT))
                except Exception:  # noqa: BLE001 — the peer may already be gone
                    pass
            await self._teardown(f"logout: {self.last_logout_text}", logged_out=True)
            return
        if msg.msg_type == MSG_REJECT:
            self._log(f"fix: session reject of #{msg.get('45')}: {msg.get('58', '')} (tag {msg.get('371', '-')})")
            return
        if msg.msg_type == MSG_LOGON:
            return
        # application
        self._pending_test_request = None
        if self._on_app is not None:
            result = self._on_app(msg)
            if asyncio.iscoroutine(result):
                await result

    async def _answer_resend(self, begin: int, end: int) -> None:
        """We never replay application messages (orders must not be re-sent); gap-fill the whole range."""
        next_seq = self._store["out"]
        await self._send(FixMessage(MSG_SEQUENCE_RESET, [("123", "Y"), ("36", str(next_seq))]), seq=begin, poss_dup=True)

    async def _teardown(self, why: str, logged_out: bool = False) -> None:
        if self.state is SessionState.DISCONNECTED and self._writer is None:
            return
        if logged_out:
            self._set_state(SessionState.LOGGED_OUT, self.last_logout_text)
        writer, self._writer, self._reader = self._writer, None, None
        if writer is not None:
            writer.close()
        current = asyncio.current_task()
        for task in self._tasks:
            if task is not current:
                task.cancel()
        self._tasks = []
        if self._logon_done and not self._logon_done.done():
            self._logon_done.set_result(False)
        self._set_state(SessionState.DISCONNECTED, why)

    def _set_state(self, state: SessionState, text: str = "") -> None:
        if state is self.state and not text:
            return
        self.state = state
        self._log(f"fix: {state.value}{': ' + text if text else ''}")
        if self._on_state is not None:
            self._on_state(state, text)
