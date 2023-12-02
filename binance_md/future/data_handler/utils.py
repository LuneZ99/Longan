from binance_md.future.api_wrapper import BinanceMarketDataAPIUtils

stg = BinanceMarketDataAPIUtils()
future_symbols = [x.lower() for x in stg.symbol_all]
