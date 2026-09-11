"""Balances on Bitfinex are FXCH's published trade limits, not venue wallets. The budget checker
caps an order candidate to the account's max buy / max sell for that instrument, and the connector's
balances are derived from the same rows so the script's inventory logic and the dashboard have numbers."""

from decimal import Decimal

import pytest
from bidict import bidict

from hummingbot.connector.exchange.bitfinex.bitfinex_exchange import BitfinexExchange
from hummingbot.connector.exchange.bitfinex.bitfinex_trade_limits import TradeLimit, TradeLimitBudgetChecker
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate


@pytest.fixture
def exchange():
    ex = BitfinexExchange(bitfinex_api_key="", bitfinex_secret_key="", bitfinex_fix_username="", bitfinex_fix_password="",
                          bitfinex_fix_sender_comp_id="", trading_pairs=["BTC-USDT", "ETH-USDT"], trading_required=False)
    ex._set_trading_pair_symbol_map(bidict({"tBTCUST": "BTC-USDT", "tETHUST": "ETH-USDT"}))
    ex._apply_trade_limits({"BTC-USDT": TradeLimit(max_buy=Decimal("0.5"), max_sell=Decimal("0.2")),
                            "ETH-USDT": TradeLimit(max_buy=Decimal("3"), max_sell=Decimal("0"))},
                           mid_prices={"BTC-USDT": Decimal("77000"), "ETH-USDT": Decimal("2400")})
    return ex


def candidate(pair, side, amount, price="77000"):
    return OrderCandidate(trading_pair=pair, is_maker=True, order_type=OrderType.LIMIT_MAKER, order_side=side,
                          amount=Decimal(amount), price=Decimal(price))


def test_the_checker_is_the_trade_limit_one(exchange):
    assert isinstance(exchange.budget_checker, TradeLimitBudgetChecker)


def test_a_buy_within_max_buy_passes_untouched(exchange):
    out = exchange.budget_checker.adjust_candidates([candidate("BTC-USDT", TradeType.BUY, "0.1")])
    assert out[0].amount == Decimal("0.1")


def test_a_buy_above_max_buy_is_shrunk_to_it(exchange):
    out = exchange.budget_checker.adjust_candidates([candidate("BTC-USDT", TradeType.BUY, "0.9")], all_or_none=False)
    assert out[0].amount == Decimal("0.5")


def test_a_sell_above_max_sell_is_shrunk_and_a_zero_allowance_means_zero(exchange):
    out = exchange.budget_checker.adjust_candidates([candidate("BTC-USDT", TradeType.SELL, "0.3"),
                                                     candidate("ETH-USDT", TradeType.SELL, "1", price="2400")],
                                                    all_or_none=False)
    assert out[0].amount == Decimal("0.2")
    assert out[1].amount == Decimal("0")


def test_all_or_none_drops_a_candidate_that_would_be_shrunk(exchange):
    out = exchange.budget_checker.adjust_candidates([candidate("BTC-USDT", TradeType.BUY, "0.9")], all_or_none=True)
    assert out[0].amount == Decimal("0")


def test_two_buys_on_one_instrument_share_the_allowance(exchange):
    out = exchange.budget_checker.adjust_candidates([candidate("BTC-USDT", TradeType.BUY, "0.3"),
                                                     candidate("BTC-USDT", TradeType.BUY, "0.3")], all_or_none=False)
    assert [c.amount for c in out] == [Decimal("0.3"), Decimal("0.2")]


def test_a_pair_without_a_published_limit_gets_nothing(exchange):
    out = exchange.budget_checker.adjust_candidates([candidate("XRP-USDT", TradeType.BUY, "10", price="1.4")])
    assert out[0].amount == Decimal("0")


def test_balances_are_derived_from_the_limits_for_the_inventory_logic(exchange):
    assert exchange.get_balance("BTC") == Decimal("0.2")                       # what we may still sell
    assert exchange.get_available_balance("BTC") == Decimal("0.2")
    assert exchange.get_balance("ETH") == Decimal("0")
    assert exchange.get_balance("USDT") == Decimal("0.5") * 77000 + Decimal("3") * 2400  # what we may still buy, in quote
