"""The ABOS client: logs in as the account's customer user, keeps the token fresh, reads the
account's published trade limits (`GET /api/v1/account/{id}/trade-limits`, bearer token)."""

import json
import time
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses

from hummingbot.connector.exchange.bitfinex.bitfinex_trade_limits import AbosTradeLimitsClient, TradeLimit

BASE = "https://demo1.abos.baxtech.hu"


def jwt_with_exp(exp):
    import base64
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp, "sub": "u"}).encode()).decode().rstrip("=")
    return f"eyJhbGciOiJub25lIn0.{payload}."


class TestAbosTradeLimitsClient(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        super().setUp()
        self.client = AbosTradeLimitsClient(base_url=BASE, username="bot@fxch", password="pw", abos_id=42297)

    @aioresponses()
    async def test_logs_in_then_reads_spot_limits_by_hummingbot_pair(self, mock_api):
        mock_api.post(f"{BASE}/api/v1/auth/login", body=json.dumps({
            "accessToken": jwt_with_exp(time.time() + 3600), "idToken": "id", "refreshToken": "r1"}))
        mock_api.get(f"{BASE}/api/v1/account/42297/trade-limits", body=json.dumps({"entries": [
            {"type": "SPOT", "symbol": "BTCUSD", "maxBuy": 0.5, "maxSell": -0.2},
            {"type": "SPOT", "symbol": "BTCUST", "maxBuy": 0.4, "maxSell": -0.1},
            {"type": "FUTURES", "symbol": "BTCUSDT", "platform": "KUCOIN", "maxBuy": 9, "maxSell": -9},
        ]}))
        limits = await self.client.fetch()
        self.assertEqual({"BTC-USD": TradeLimit(Decimal("0.5"), Decimal("0.2")),
                          "BTC-USDT": TradeLimit(Decimal("0.4"), Decimal("0.1"))}, limits)
        login = [r for r in mock_api.requests if r[0] == "POST"]
        self.assertEqual({"username": "bot@fxch", "password": "pw"}, mock_api.requests[login[0]][0].kwargs["json"])
        get = mock_api.requests[[r for r in mock_api.requests if r[0] == "GET"][0]][0]
        self.assertTrue(get.kwargs["headers"]["Authorization"].startswith("Bearer eyJ"))

    @aioresponses()
    async def test_an_expiring_token_is_refreshed_before_the_next_read(self, mock_api):
        mock_api.post(f"{BASE}/api/v1/auth/login", body=json.dumps({
            "accessToken": jwt_with_exp(time.time() + 10), "idToken": "id", "refreshToken": "r1"}))
        mock_api.post(f"{BASE}/api/v1/auth/refresh", body=json.dumps({
            "accessToken": jwt_with_exp(time.time() + 3600), "idToken": "id2"}))
        mock_api.get(f"{BASE}/api/v1/account/42297/trade-limits", body=json.dumps({"entries": []}), repeat=True)
        await self.client.fetch()
        await self.client.fetch()
        posts = [r for r in mock_api.requests if r[0] == "POST"]
        self.assertEqual({"login", "refresh"}, {str(r[1]).rsplit("/", 1)[-1] for r in posts})
        refresh = mock_api.requests[[r for r in posts if str(r[1]).endswith("refresh")][0]][0]
        self.assertEqual({"refreshToken": "r1"}, refresh.kwargs["json"])

    @aioresponses()
    async def test_a_rejected_login_is_reported_without_the_password(self, mock_api):
        mock_api.post(f"{BASE}/api/v1/auth/login", status=500, body=json.dumps({"error": "Internal Server Error"}))
        with self.assertRaises(RuntimeError) as err:
            await self.client.fetch()
        self.assertIn("login", str(err.exception).lower())
        self.assertNotIn("pw", str(err.exception))

    @aioresponses()
    async def test_a_401_on_the_read_forces_a_fresh_login_once(self, mock_api):
        mock_api.post(f"{BASE}/api/v1/auth/login", body=json.dumps({
            "accessToken": jwt_with_exp(time.time() + 3600), "idToken": "id", "refreshToken": "r1"}), repeat=True)
        mock_api.get(f"{BASE}/api/v1/account/42297/trade-limits", status=401, body="{}")
        mock_api.get(f"{BASE}/api/v1/account/42297/trade-limits", body=json.dumps({"entries": [
            {"type": "SPOT", "symbol": "ETHUST", "maxBuy": 3, "maxSell": 0}]}))
        limits = await self.client.fetch()
        self.assertEqual({"ETH-USDT": TradeLimit(Decimal("3"), Decimal("0"))}, limits)
        login_key = [r for r in mock_api.requests if r[0] == "POST"][0]
        self.assertEqual(2, len(mock_api.requests[login_key]))  # first token refused → one fresh login
