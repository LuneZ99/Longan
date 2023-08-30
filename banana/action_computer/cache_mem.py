from diskcache import Cache


# cache_folder = "../market_feed/cache"
# symbol = "aaveusdt"
# event = "bookTicker"

# dc = Cache("/tmp/binance_cache/api3usdt@aggTrade")
dc = Cache("/tmp/binance_cache/api3usdt@bookTicker")

print(dc.peek())

