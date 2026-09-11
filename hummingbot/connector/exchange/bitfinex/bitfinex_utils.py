from decimal import Decimal
from typing import Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDT"

# Bitfinex carries no fee on FIX execution reports; the fee is a configured schema (override with
# Hummingbot's fee-override settings). These are Bitfinex's published base rates.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.002"),
)

AMOUNT_QUANTUM = Decimal("0.00000001")  # amounts: 8 decimals
PRICE_SIGNIFICANT_DIGITS = 5  # Bitfinex prices: 5 significant digits

# Bitfinex's currency codes that differ from the market's: USDT is "UST" on Bitfinex.
_HB_TO_BFX_CURRENCY: Dict[str, str] = {"USDT": "UST"}
_BFX_TO_HB_CURRENCY: Dict[str, str] = {v: k for k, v in _HB_TO_BFX_CURRENCY.items()}
_KNOWN_QUOTES = ("USDT", "USD", "BTC", "ETH", "EUR", "GBP", "JPY", "TESTUSD", "TESTBTC")


def _bfx_currency(hb: str) -> str:
    return _HB_TO_BFX_CURRENCY.get(hb, hb)


def _hb_currency(bfx: str) -> str:
    return _BFX_TO_HB_CURRENCY.get(bfx, bfx)


def hb_currency(bfx: str) -> str:
    """Bitfinex currency code -> the market's (``UST`` -> ``USDT``)."""
    return _hb_currency(bfx)


def fix_symbol(trading_pair: str) -> str:
    """``BTC-USDT`` -> ``BTCUST``; codes longer than three characters take Bitfinex's ``:`` form (``AAVE:UST``)."""
    base, quote = trading_pair.split("-")
    base, quote = _bfx_currency(base), _bfx_currency(quote)
    return f"{base}:{quote}" if len(base) > 3 or len(quote) > 3 else f"{base}{quote}"


def rest_symbol(trading_pair: str) -> str:
    """The REST/websocket form is the FIX form with a ``t`` prefix: ``tBTCUST``."""
    return "t" + fix_symbol(trading_pair)


def trading_pair_from_symbol(symbol: str) -> str:
    """``tBTCUST`` / ``BTCUST`` / ``tAAVE:UST`` -> ``BTC-USDT``."""
    raw = symbol[1:] if symbol.startswith("t") and ":" not in symbol[:2] and len(symbol) > 6 else symbol
    if raw.startswith("t") and ":" in raw:
        raw = raw[1:]
    if ":" in raw:
        base, quote = raw.split(":", 1)
    elif len(raw) == 6:
        base, quote = raw[:3], raw[3:]
    else:
        # no separator and not 3+3: try the known quote currencies, longest first
        for q in sorted((_bfx_currency(q) for q in _KNOWN_QUOTES), key=len, reverse=True):
            if raw.endswith(q) and len(raw) > len(q):
                base, quote = raw[:-len(q)], q
                break
        else:
            raise ValueError(f"cannot split Bitfinex symbol {symbol} into base and quote")
    return f"{_hb_currency(base)}-{_hb_currency(quote)}"


def price_quantum(price: Decimal) -> Decimal:
    """The smallest price step at this price level under the 5-significant-digit rule."""
    if price <= 0:
        return Decimal("0.00001")
    exponent = price.adjusted() - (PRICE_SIGNIFICANT_DIGITS - 1)
    return Decimal(1).scaleb(exponent)


class BitfinexConfigMap(BaseConnectorConfigMap):
    connector: str = "bitfinex"
    bitfinex_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={"prompt": "Enter your Bitfinex REST API key (read-only: balances/fees/history)",
                           "is_secure": True, "is_connect_key": True, "prompt_on_new": True},
    )
    bitfinex_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={"prompt": "Enter your Bitfinex REST API secret", "is_secure": True,
                           "is_connect_key": True, "prompt_on_new": True},
    )
    bitfinex_fix_username: SecretStr = Field(
        default=...,
        json_schema_extra={"prompt": "Enter your Bitfinex FIX session username", "is_secure": True,
                           "is_connect_key": True, "prompt_on_new": True},
    )
    bitfinex_fix_password: SecretStr = Field(
        default=...,
        json_schema_extra={"prompt": "Enter your Bitfinex FIX session password", "is_secure": True,
                           "is_connect_key": True, "prompt_on_new": True},
    )
    bitfinex_fix_sender_comp_id: SecretStr = Field(
        default=...,
        json_schema_extra={"prompt": "Enter your Bitfinex FIX SenderCompID", "is_secure": True,
                           "is_connect_key": True, "prompt_on_new": True},
    )
    bitfinex_fix_host: str = Field(
        default="",
        json_schema_extra={"prompt": "Bitfinex FIX gateway host", "is_secure": False, "is_connect_key": True,
                           "prompt_on_new": True},
    )
    bitfinex_fix_port: int = Field(
        default=0,
        json_schema_extra={"prompt": "Bitfinex FIX gateway port", "is_secure": False, "is_connect_key": True,
                           "prompt_on_new": True},
    )
    # balances = ABOS's published trade limits, read as a customer user scoped to the bot's account
    bitfinex_abos_url: str = Field(
        default="",
        json_schema_extra={"prompt": "ABOS web app base URL (e.g. https://demo1.abos.baxtech.hu)", "is_secure": False,
                           "is_connect_key": True, "prompt_on_new": True},
    )
    bitfinex_abos_username: SecretStr = Field(
        default=SecretStr(""),
        json_schema_extra={"prompt": "ABOS customer user for the bot's account", "is_secure": True,
                           "is_connect_key": True, "prompt_on_new": True},
    )
    bitfinex_abos_password: SecretStr = Field(
        default=SecretStr(""),
        json_schema_extra={"prompt": "ABOS customer user's password", "is_secure": True, "is_connect_key": True,
                           "prompt_on_new": True},
    )
    bitfinex_abos_account_id: int = Field(
        default=0,
        json_schema_extra={"prompt": "ABOS account id the bot trades as (e.g. 42297)", "is_secure": False,
                           "is_connect_key": True, "prompt_on_new": True},
    )
    model_config = ConfigDict(title="bitfinex")


KEYS = BitfinexConfigMap.model_construct()
