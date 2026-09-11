"""Bitfinex REST v2 authentication: nonce + HMAC-SHA384 over '/api' + path + nonce + body."""

import hashlib
import hmac
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.exchange.bitfinex.bitfinex_auth import BitfinexAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class TestBitfinexAuth(IsolatedAsyncioWrapperTestCase):

    async def test_headers_carry_key_nonce_and_signature_over_path_nonce_body(self):
        auth = BitfinexAuth(api_key="key-1", secret_key="secret-1", nonce_provider=lambda: 1700000000000000)
        body = json.dumps({"id": [123]})
        request = RESTRequest(method=RESTMethod.POST, url="https://api.bitfinex.com/v2/auth/r/orders/tBTCUST/hist",
                              data=body, is_auth_required=True)
        signed = await auth.rest_authenticate(request)
        expected = hmac.new(b"secret-1", b"/api/v2/auth/r/orders/tBTCUST/hist" + b"1700000000000000" + body.encode(),
                            hashlib.sha384).hexdigest()
        self.assertEqual("key-1", signed.headers["bfx-apikey"])
        self.assertEqual("1700000000000000", signed.headers["bfx-nonce"])
        self.assertEqual(expected, signed.headers["bfx-signature"])
        self.assertEqual("application/json", signed.headers["Content-Type"])

    async def test_a_request_without_a_body_signs_an_empty_body_and_nonces_increase(self):
        nonces = iter([5, 5, 6])
        auth = BitfinexAuth(api_key="k", secret_key="s", nonce_provider=lambda: next(nonces))
        a = await auth.rest_authenticate(RESTRequest(method=RESTMethod.POST, url="https://api.bitfinex.com/v2/auth/r/summary",
                                                     is_auth_required=True))
        b = await auth.rest_authenticate(RESTRequest(method=RESTMethod.POST, url="https://api.bitfinex.com/v2/auth/r/summary",
                                                     is_auth_required=True))
        self.assertEqual("{}", a.data)
        self.assertLess(int(a.headers["bfx-nonce"]), int(b.headers["bfx-nonce"]))
