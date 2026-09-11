"""Balances on Bitfinex are not wallets: FXCH's account holds no assets on the venue. Bitfinex
pre-trade-checks every order against the max buy / max sell that FXCH streams to it, and the same
numbers are published to the customer by ABOS. So the connector reads its allowance from ABOS
(the customer door, `GET /api/v1/account/{id}/trade-limits`, bearer token of a customer user scoped
to the account) and sizes orders against it — exactly as a human FXCH customer would.

A Bitfinex "insufficient balance" reject after this check therefore means ABOS's published limit
and Bitfinex's enforced limit disagree: a finding, not noise.
"""

from __future__ import annotations

import base64
import json
import time
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional

import aiohttp

from hummingbot.connector.budget_checker import BudgetChecker
from hummingbot.connector.exchange.bitfinex import bitfinex_utils as utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate

LOGIN_PATH = "/api/v1/auth/login"
REFRESH_PATH = "/api/v1/auth/refresh"
TRADE_LIMITS_PATH = "/api/v1/account/{abos_id}/trade-limits"
TOKEN_REFRESH_MARGIN_S = 60


@dataclass(frozen=True)
class TradeLimit:
    max_buy: Decimal   # base units the account may still buy on the instrument
    max_sell: Decimal  # base units it may still sell (ABOS publishes this signed <= 0; stored as a magnitude)


def _jwt_expiry(token: str) -> float:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return float(json.loads(base64.urlsafe_b64decode(payload)).get("exp", 0))
    except Exception:  # noqa: BLE001 — an unreadable token just gets refreshed early
        return 0.0


class AbosTradeLimitsClient:
    """Logs in as the account's customer user, keeps the access token fresh, reads the SPOT limits."""

    def __init__(self, base_url: str, username: str, password: str, abos_id: int, timeout_s: float = 20.0) -> None:
        self._base = base_url.rstrip("/")
        self._username, self._password = username, password
        self._abos_id = int(abos_id)
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._expires_at = 0.0

    async def fetch(self) -> Dict[str, TradeLimit]:
        """SPOT rows keyed by Hummingbot pair (``BTC-USDT``); FUTURES rows are ignored here."""
        token = await self._token()
        payload = await self._get_limits(token)
        if payload is None:  # 401: the token was refused — log in afresh once
            self._access_token = None
            payload = await self._get_limits(await self._token())
            if payload is None:
                raise RuntimeError("ABOS refused the trade-limits read twice (401)")
        limits: Dict[str, TradeLimit] = {}
        for row in payload.get("entries", []):
            if row.get("type") != "SPOT":
                continue
            try:
                pair = utils.trading_pair_from_symbol(str(row["symbol"]))
            except ValueError:
                continue
            limits[pair] = TradeLimit(max_buy=max(Decimal(str(row["maxBuy"])), Decimal("0")),
                                      max_sell=abs(Decimal(str(row["maxSell"]))))
        return limits

    async def _token(self) -> str:
        now = time.time()
        if self._access_token and now < self._expires_at - TOKEN_REFRESH_MARGIN_S:
            return self._access_token
        if self._access_token and self._refresh_token:
            try:
                data = await self._post(REFRESH_PATH, {"refreshToken": self._refresh_token})
                self._set_tokens(data)
                return self._access_token
            except RuntimeError:
                pass  # fall through to a fresh login
        data = await self._post(LOGIN_PATH, {"username": self._username, "password": self._password})
        self._set_tokens(data)
        return self._access_token

    def _set_tokens(self, data: dict) -> None:
        token = data.get("accessToken")
        if not token:
            raise RuntimeError("ABOS login answered without an access token")
        self._access_token = token
        self._refresh_token = data.get("refreshToken", self._refresh_token)
        self._expires_at = _jwt_expiry(token) or (time.time() + 300)

    async def _post(self, path: str, body: dict) -> dict:
        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            async with session.post(self._base + path, json=body) as resp:
                if resp.status != 200:
                    what = "login" if path == LOGIN_PATH else "token refresh"
                    raise RuntimeError(f"ABOS {what} failed with HTTP {resp.status} for user {self._username}")
                return await resp.json()

    async def _get_limits(self, token: str) -> Optional[dict]:
        url = self._base + TRADE_LIMITS_PATH.format(abos_id=self._abos_id)
        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            async with session.get(url, headers={"Authorization": f"Bearer {token}"}) as resp:
                if resp.status == 401:
                    return None
                if resp.status != 200:
                    raise RuntimeError(f"ABOS trade-limits read failed with HTTP {resp.status} for account {self._abos_id}")
                return await resp.json()


class TradeLimitBudgetChecker(BudgetChecker):
    """Caps each candidate to the account's published allowance for that instrument and side."""

    def __init__(self, exchange, limits_getter) -> None:
        super().__init__(exchange)
        self._limits_getter = limits_getter
        self._used: Dict[tuple, Decimal] = defaultdict(lambda: Decimal("0"))

    def reset_locked_collateral(self):
        super().reset_locked_collateral()
        self._used.clear()

    def adjust_candidate_and_lock_available_collateral(self, order_candidate: OrderCandidate,
                                                       all_or_none: bool = True) -> OrderCandidate:
        limits: Dict[str, TradeLimit] = self._limits_getter()
        limit = limits.get(order_candidate.trading_pair)
        key = (order_candidate.trading_pair, order_candidate.order_side)
        allowance = Decimal("0")
        if limit is not None:
            allowance = limit.max_buy if order_candidate.order_side is TradeType.BUY else limit.max_sell
        remaining = max(allowance - self._used[key], Decimal("0"))
        candidate = order_candidate
        if candidate.amount > remaining:
            candidate.amount = Decimal("0") if all_or_none else remaining
        candidate = self._exchange_quantize(candidate)
        self._used[key] += candidate.amount
        return self.populate_collateral_entries(candidate)

    def _exchange_quantize(self, candidate: OrderCandidate) -> OrderCandidate:
        if candidate.amount > 0:
            candidate.amount = self._exchange.quantize_order_amount(candidate.trading_pair, candidate.amount)
        return candidate

    def adjust_candidate(self, order_candidate: OrderCandidate, all_or_none: bool = True) -> OrderCandidate:
        self.reset_locked_collateral()
        return self.adjust_candidate_and_lock_available_collateral(order_candidate, all_or_none)
