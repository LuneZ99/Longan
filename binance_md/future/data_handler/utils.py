from binance_md.future.api_wrapper import BinanceMarketDataAPIUtils

FT = BinanceMarketDataAPIUtils()
future_symbols = [x.lower() for x in FT.symbol_all]
