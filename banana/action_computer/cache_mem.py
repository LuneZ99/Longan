from diskcache import Cache
import pandas as pd

# cache_folder = "../market_feed/cache"
# symbol = "aaveusdt"
# event = "bookTicker"

# dc = Cache("/tmp/binance_cache/api3usdt@aggTrade")
dc = Cache("/tmp/binance_cache/kline_1m")

print(dc.peek())

print(pd.DataFrame([dc[x] for x in dc.iterkeys()]))
