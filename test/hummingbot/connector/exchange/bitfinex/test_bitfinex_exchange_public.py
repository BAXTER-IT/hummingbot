"""The connector's public surface: symbols, trading rules with Bitfinex's price rule, last price, health.
Recorded Bitfinex responses live in fixtures/ (trimmed to a few pairs)."""

import json
from decimal import Decimal
from pathlib import Path
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.connector.exchange.bitfinex import bitfinex_constants as CONSTANTS, bitfinex_web_utils as web_utils
from hummingbot.connector.exchange.bitfinex.bitfinex_exchange import BitfinexExchange
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.network_iterator import NetworkStatus

FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


class TestBitfinexExchangePublic(IsolatedAsyncioWrapperTestCase):

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.exchange = BitfinexExchange(
            bitfinex_api_key="", bitfinex_secret_key="", bitfinex_fix_username="", bitfinex_fix_password="",
            bitfinex_fix_sender_comp_id="", bitfinex_fix_host="", bitfinex_fix_port=0,
            trading_pairs=["BTC-USDT"], trading_required=False,
        )

    async def test_symbol_map_comes_from_the_pair_list_with_usdt_as_ust(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(fixture("pub_list_pair_exchange.json"))
        mapping = await self.exchange.trading_pair_symbol_map()
        self.assertEqual("BTC-USDT", mapping["tBTCUST"])
        self.assertEqual("AAVE-USDT", mapping["tAAVE:UST"])
        self.assertEqual("BTC-USD", mapping["tBTCUSD"])
        self.assertNotIn("tTESTBTC:TESTUSD", mapping)  # test pairs are not offered

    async def test_symbol_map_also_accepts_the_pair_info_shape_the_rules_poll_feeds_it(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(fixture("pub_info_pair.json"))
        mapping = await self.exchange.trading_pair_symbol_map()
        self.assertEqual("ETH-USDT", mapping["tETHUST"])

    async def test_trading_rules_take_min_and_max_size_from_pair_info(self):
        self.exchange._set_trading_pair_symbol_map(bidict({"tBTCUST": "BTC-USDT", "tETHUST": "ETH-USDT"}))
        rules = await self.exchange._format_trading_rules(fixture("pub_info_pair.json"))
        by_pair = {r.trading_pair: r for r in rules}
        self.assertEqual({"BTC-USDT", "ETH-USDT"}, set(by_pair))
        self.assertEqual(Decimal("0.00004"), by_pair["BTC-USDT"].min_order_size)
        self.assertEqual(Decimal("2000.0"), by_pair["BTC-USDT"].max_order_size)
        self.assertEqual(Decimal("0.0008"), by_pair["ETH-USDT"].min_order_size)
        self.assertEqual(Decimal("0.00000001"), by_pair["BTC-USDT"].min_base_amount_increment)
        self.assertEqual(Decimal("0"), by_pair["BTC-USDT"].min_notional_size)

    async def test_price_quantum_follows_five_significant_digits_not_a_fixed_increment(self):
        self.exchange._set_trading_pair_symbol_map(bidict({"tBTCUST": "BTC-USDT"}))
        self.exchange._trading_rules = {r.trading_pair: r for r in
                                        await self.exchange._format_trading_rules(fixture("pub_info_pair.json"))}
        self.assertEqual(Decimal("1"), self.exchange.get_order_price_quantum("BTC-USDT", Decimal("77128.4")))
        self.assertEqual(Decimal("0.1"), self.exchange.get_order_price_quantum("BTC-USDT", Decimal("2441.82")))
        self.assertEqual(Decimal("77128"), self.exchange.quantize_order_price("BTC-USDT", Decimal("77128.4")))
        self.assertEqual(Decimal("2441.8"), self.exchange.quantize_order_price("BTC-USDT", Decimal("2441.82")))
        self.assertEqual(Decimal("0.00000001"), self.exchange.get_order_size_quantum("BTC-USDT", Decimal("1")))

    @aioresponses()
    async def test_last_traded_price_is_the_tickers_last_field(self, mock_api):
        self.exchange._set_trading_pair_symbol_map(bidict({"tBTCUST": "BTC-USDT"}))
        mock_api.get(web_utils.public_rest_url(CONSTANTS.TICKER_PATH.format(symbol="tBTCUST")),
                     body=json.dumps(fixture("ticker_tBTCUST.json")))
        self.assertEqual(77134.0, await self.exchange._get_last_traded_price("BTC-USDT"))

    @aioresponses()
    async def test_network_check_uses_platform_status(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.PLATFORM_STATUS_PATH)
        mock_api.get(url, body="[1]")
        self.assertEqual(NetworkStatus.CONNECTED, await self.exchange.check_network())
        mock_api.get(url, body="[0]")
        self.assertEqual(NetworkStatus.NOT_CONNECTED, await self.exchange.check_network())

    def test_supported_order_types_and_ids(self):
        self.assertEqual([OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET], self.exchange.supported_order_types())
        self.assertEqual(36, self.exchange.client_order_id_max_length)
        self.assertEqual("bitfinex", self.exchange.name)
        self.assertFalse(self.exchange.is_cancel_request_in_exchange_synchronous)
