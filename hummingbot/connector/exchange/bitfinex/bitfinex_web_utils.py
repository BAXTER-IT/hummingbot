import time
from typing import Optional

from hummingbot.connector.exchange.bitfinex import bitfinex_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.PUBLIC_REST_URL + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.PRIVATE_REST_URL + path_url


def build_api_factory(throttler: Optional[AsyncThrottler] = None, auth: Optional[AuthBase] = None,
                      **_ignored) -> WebAssistantsFactory:
    """No time-synchronizer pre-processor: Bitfinex REST signs with a client nonce and FIX checks
    SendingTime against the gateway clock, so the host clock (NTP) is the reference."""
    return WebAssistantsFactory(throttler=throttler or create_throttler(), auth=auth)


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(throttler: Optional[AsyncThrottler] = None,
                                  domain: str = CONSTANTS.DEFAULT_DOMAIN) -> float:
    return time.time()
