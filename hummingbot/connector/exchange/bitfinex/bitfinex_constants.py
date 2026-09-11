"""Bitfinex constants: public REST/WS for market data and trading rules, private REST for the few
authenticated reads, and the FIX 4.4 gateway for every order message (spec v1.21)."""

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

EXCHANGE_NAME = "bitfinex"
DEFAULT_DOMAIN = "main"

PUBLIC_REST_URL = "https://api-pub.bitfinex.com"
PRIVATE_REST_URL = "https://api.bitfinex.com"
PUBLIC_WS_URL = "wss://api-pub.bitfinex.com/ws/2"

# public REST
PLATFORM_STATUS_PATH = "/v2/platform/status"
PAIR_LIST_PATH = "/v2/conf/pub:list:pair:exchange"
PAIR_INFO_PATH = "/v2/conf/pub:info:pair"
BOOK_PATH = "/v2/book/{symbol}/P0"
TICKER_PATH = "/v2/ticker/{symbol}"
# private REST (reads only; orders never go here)
SUMMARY_PATH = "/v2/auth/r/summary"
ACTIVE_ORDERS_PATH = "/v2/auth/r/orders/{symbol}"
ORDER_HISTORY_PATH = "/v2/auth/r/orders/{symbol}/hist"
ORDER_TRADES_PATH = "/v2/auth/r/order/{symbol}:{order_id}/trades"

# pub:info:pair row: [symbol, [.., .., .., min_order_size, max_order_size, .., .., .., initial_margin, min_margin, .., ..]]
PAIR_INFO_MIN_ORDER_SIZE = 3
PAIR_INFO_MAX_ORDER_SIZE = 4
# order row: [ID, GID, CID, SYMBOL, MTS_CREATE, MTS_UPDATE, AMOUNT, AMOUNT_ORIG, TYPE, TYPE_PREV, _, _, FLAGS, STATUS,
#             _, _, PRICE, PRICE_AVG, ...]; trade row: [ID, PAIR, MTS_CREATE, ORDER_ID, EXEC_AMOUNT, EXEC_PRICE,
#             ORDER_TYPE, ORDER_PRICE, MAKER, FEE, FEE_CURRENCY]
ORDER_ID, ORDER_SYMBOL, ORDER_MTS_UPDATE, ORDER_AMOUNT, ORDER_AMOUNT_ORIG, ORDER_STATUS, ORDER_PRICE, ORDER_PRICE_AVG = \
    0, 3, 5, 6, 7, 13, 16, 17
TRADE_ROW_ID, TRADE_ROW_MTS, TRADE_ROW_ORDER_ID, TRADE_ROW_AMOUNT, TRADE_ROW_PRICE, TRADE_ROW_MAKER, TRADE_ROW_FEE, \
    TRADE_ROW_FEE_CCY = 0, 2, 3, 4, 5, 8, 9, 10
# ticker: [bid, bid_size, ask, ask_size, daily_change, daily_change_rel, last_price, volume, high, low, ...]
TICKER_BID, TICKER_ASK, TICKER_LAST = 0, 2, 6
# book P0 row: [price, count, amount]; amount > 0 bid, < 0 ask, count == 0 delete
BOOK_PRICE, BOOK_COUNT, BOOK_AMOUNT = 0, 1, 2
# trade row: [id, mts, amount, price]
TRADE_ID, TRADE_MTS, TRADE_AMOUNT, TRADE_PRICE = 0, 1, 2, 3
BOOK_DEPTH = 100

WS_HEARTBEAT = "hb"
WS_BOOK_CHANNEL = "book"
WS_TRADES_CHANNEL = "trades"
WS_PING_INTERVAL_S = 20.0

# FIX gateway (spec v1.21); host/port/comp ids come from the connector config
FIX_TARGET_COMP_ID = "BfxComp"
FIX_DEFAULT_HEARTBEAT_S = 10
FIX_CLIENT_ORDER_ID_MAX_LENGTH = 36
FIX_ORDER_FLAG_HIDDEN = 64
FIX_ORDER_FLAG_REDUCE_ONLY = 1024
FIX_ORDER_FLAG_POST_ONLY = 4096
FIX_TIF_SESSION, FIX_TIF_IOC, FIX_TIF_FOK = "0", "3", "4"

MAX_ORDER_ID_LEN = FIX_CLIENT_ORDER_ID_MAX_LENGTH
HBOT_ORDER_ID_PREFIX = ""

# Bitfinex public: 90 req/min per endpoint family; private: 90 req/min. Conservative single buckets.
PUBLIC_LIMIT_ID = "bitfinex-public"
PRIVATE_LIMIT_ID = "bitfinex-private"
RATE_LIMITS = [
    RateLimit(limit_id=PUBLIC_LIMIT_ID, limit=90, time_interval=60),
    RateLimit(limit_id=PRIVATE_LIMIT_ID, limit=90, time_interval=60),
    RateLimit(limit_id=PLATFORM_STATUS_PATH, limit=90, time_interval=60, linked_limits=[LinkedLimitWeightPair(PUBLIC_LIMIT_ID)]),
    RateLimit(limit_id=PAIR_LIST_PATH, limit=90, time_interval=60, linked_limits=[LinkedLimitWeightPair(PUBLIC_LIMIT_ID)]),
    RateLimit(limit_id=PAIR_INFO_PATH, limit=90, time_interval=60, linked_limits=[LinkedLimitWeightPair(PUBLIC_LIMIT_ID)]),
    RateLimit(limit_id=BOOK_PATH, limit=90, time_interval=60, linked_limits=[LinkedLimitWeightPair(PUBLIC_LIMIT_ID)]),
    RateLimit(limit_id=TICKER_PATH, limit=90, time_interval=60, linked_limits=[LinkedLimitWeightPair(PUBLIC_LIMIT_ID)]),
    RateLimit(limit_id=SUMMARY_PATH, limit=90, time_interval=60, linked_limits=[LinkedLimitWeightPair(PRIVATE_LIMIT_ID)]),
    RateLimit(limit_id=ACTIVE_ORDERS_PATH, limit=90, time_interval=60, linked_limits=[LinkedLimitWeightPair(PRIVATE_LIMIT_ID)]),
    RateLimit(limit_id=ORDER_HISTORY_PATH, limit=90, time_interval=60, linked_limits=[LinkedLimitWeightPair(PRIVATE_LIMIT_ID)]),
    RateLimit(limit_id=ORDER_TRADES_PATH, limit=90, time_interval=60, linked_limits=[LinkedLimitWeightPair(PRIVATE_LIMIT_ID)]),
]
