"""Symbol conversion and Bitfinex's price-precision rule."""

from decimal import Decimal

import pytest

from hummingbot.connector.exchange.bitfinex import bitfinex_utils as utils


@pytest.mark.parametrize("hb_pair, rest_symbol, fix_symbol", [
    ("BTC-USDT", "tBTCUST", "BTCUST"),
    ("ETH-USD", "tETHUSD", "ETHUSD"),
    ("AAVE-USDT", "tAAVE:UST", "AAVE:UST"),
    ("XRP-USDT", "tXRPUST", "XRPUST"),
    ("TESTBTC-TESTUSD", "tTESTBTC:TESTUSD", "TESTBTC:TESTUSD"),
])
def test_symbols_convert_both_ways_with_usdt_as_ust(hb_pair, rest_symbol, fix_symbol):
    assert utils.rest_symbol(hb_pair) == rest_symbol
    assert utils.fix_symbol(hb_pair) == fix_symbol
    assert utils.trading_pair_from_symbol(rest_symbol) == hb_pair
    assert utils.trading_pair_from_symbol(fix_symbol) == hb_pair


def test_a_symbol_that_cannot_be_split_is_refused_by_name():
    with pytest.raises(ValueError) as err:
        utils.trading_pair_from_symbol("WEIRD")
    assert "WEIRD" in str(err.value)


@pytest.mark.parametrize("price, quantum", [
    ("77128", "1"), ("2441.82", "0.1"), ("0.51234", "0.00001"), ("123456", "10"), ("1.0000", "0.0001"), ("99999", "1"),
])
def test_price_quantum_is_five_significant_digits(price, quantum):
    assert utils.price_quantum(Decimal(price)) == Decimal(quantum)


def test_amount_quantum_is_eight_decimals():
    assert utils.AMOUNT_QUANTUM == Decimal("0.00000001")


def test_default_fees_and_registration_metadata_exist():
    assert utils.CENTRALIZED is True
    assert utils.EXAMPLE_PAIR == "BTC-USDT"
    assert utils.DEFAULT_FEES.maker_percent_fee_decimal == Decimal("0.001")
    assert utils.DEFAULT_FEES.taker_percent_fee_decimal == Decimal("0.002")
    fields = set(utils.KEYS.model_fields)
    assert {"bitfinex_api_key", "bitfinex_secret_key", "bitfinex_fix_username", "bitfinex_fix_password",
            "bitfinex_fix_sender_comp_id", "bitfinex_abos_url", "bitfinex_abos_username", "bitfinex_abos_password",
            "bitfinex_abos_account_id"} <= fields
