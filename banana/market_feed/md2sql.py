from base_ws_client import BaseBinanceWSClient
from banana.mysql_handler import *


class SymbolStreamMysqlHandler:

    def __init__(self, symbol):
        self.on_kline_1m = KlineHandler(symbol, 'kline_1m')
        self.on_kline_1h = KlineHandler(symbol, 'kline_1h')
        self.on_kline_8h = KlineHandler(symbol, 'kline_8h')
        self.on_agg_trade = AggTradeHandler(symbol)
        self.on_book_ticker = BookTickerHandler(symbol)


class BinanceFutureMD(BaseBinanceWSClient):
    handlers: dict[str, SymbolStreamMysqlHandler]

    def __init__(self, log_file="log.default", symbols=None, config_file=None, proxy=None, ws_trace=False, debug=False):

        super().__init__(log_file, config_file, proxy, ws_trace, debug)

        self.handlers: dict[str, SymbolStreamMysqlHandler] = {
            symbol: SymbolStreamMysqlHandler(symbol)
            for symbol in symbols
        }

    def on_agg_trade(self, symbol: str, data: dict, rec_time: int):
        self.handlers[symbol].on_agg_trade.process_line(data, rec_time)
        pass

    def on_depth20(self, symbol: str, data: dict, rec_time: int):
        pass

    def on_force_order(self, symbol: str, data: dict, rec_time: int):
        pass

    def on_kline_1m(self, symbol: str, data: dict, rec_time: int):
        self.handlers[symbol].on_kline_1m.process_line(data, rec_time)

    def on_kline_8h(self, symbol: str, data: dict, rec_time: int):
        self.handlers[symbol].on_kline_8h.process_line(data, rec_time)

    def on_kline_1h(self, symbol: str, data: dict, rec_time: int):
        self.handlers[symbol].on_kline_1h.process_line(data, rec_time)

    def on_book_ticker(self, symbol: str, data: dict, rec_time: int):
        # save all bookTicker is useless.
        # self.handlers[symbol].on_book_ticker.process_line(data, rec_time)
        pass

    def on_close(self, ws, code, message):
        pass


if __name__ == '__main__':

    # symbol for test
    symbols_all = ['ethusdt', 'btcusdt', 'bnbusdt']

    s = BinanceFutureMD(
        symbols=symbols_all,
        proxy=[
            "http://127.0.0.1:7890",
            # "http://i.**REMOVED**:7890",
        ],
        ws_trace=False
    )

    for _symbol in symbols_all:
        s.subscribe(_symbol, "aggTrade", log_interval=50000)
        s.subscribe(_symbol, "kline_1m", log_interval=10000)
        s.subscribe(_symbol, "kline_8h", log_interval=10000)
        # s.subscribe(_symbol, "depth20@100ms", log_interval=100)
        # s.subscribe(_symbol, "forceOrder", log_interval=100)
        s.subscribe(_symbol, "bookTicker", log_interval=50000)

    s.run()
