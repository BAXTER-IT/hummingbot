"""Bitfinex REST v2 authentication for the private *reads* (orders never go over REST).

Headers: ``bfx-apikey``, ``bfx-nonce`` (strictly increasing; microseconds), ``bfx-signature`` =
HMAC-SHA384(secret, "/api" + path + nonce + raw JSON body) as hex. The body is signed exactly as
sent, so a request without one carries ``{}``.
"""

import hashlib
import hmac
import time
from typing import Callable, Optional
from urllib.parse import urlparse

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


def _default_nonce() -> int:
    return int(time.time() * 1_000_000)


class BitfinexAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, nonce_provider: Optional[Callable[[], int]] = None) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._nonce_provider = nonce_provider or _default_nonce
        self._last_nonce = 0

    def _next_nonce(self) -> int:
        nonce = max(self._nonce_provider(), self._last_nonce + 1)
        self._last_nonce = nonce
        return nonce

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        if request.data is None:
            request.data = "{}"
        body = request.data if isinstance(request.data, str) else str(request.data)
        path = urlparse(request.url).path
        nonce = str(self._next_nonce())
        signature = hmac.new(self._secret_key.encode(), f"/api{path}{nonce}{body}".encode(), hashlib.sha384).hexdigest()
        headers = dict(request.headers or {})
        headers.update({"bfx-apikey": self._api_key, "bfx-nonce": nonce, "bfx-signature": signature,
                        "Content-Type": "application/json"})
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request  # the private websocket is not used; FIX is the private stream
