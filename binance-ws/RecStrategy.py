import os

from Strategy import BinanceSyncStrategy
from Handler import SymbolStreamCsvHandler


class Rec2CsvStrategy(BinanceSyncStrategy):
    handlers: dict[str, SymbolStreamCsvHandler]

    def __init__(self, parent_path, log_file, symbols=None, config_file=None, proxy=None, ws_trace=False, debug=False):
        super().__init__(log_file, config_file, proxy, ws_trace, debug)

        if symbols is None:
            symbols = ["ethusdt", "btcusdt"]
        if not os.path.exists(parent_path):
            os.makedirs(parent_path)

        self.handlers: dict[str, SymbolStreamCsvHandler] = {
            symbol: SymbolStreamCsvHandler(parent_path, symbol)
            for symbol in symbols
        }

    def on_agg_trade(self, symbol: str, name: str, data: dict, rec_time: int):
        self.handlers[symbol].on_agg_trade.process_line(data)

    def on_depth20(self, symbol: str, name: str, data: dict, rec_time: int):
        self.handlers[symbol].on_depth20.process_line(data)

    def on_force_order(self, symbol: str, name: str, data: dict, rec_time: int):
        self.handlers[symbol].on_force_order.process_line(data)

    def on_kline_1m(self, symbol: str, name: str, data: dict, rec_time: int):
        self.handlers[symbol].on_kline_1m.process_line(data)

    def on_book_ticker(self, symbol: str, name: str, data: dict, rec_time: int):
        self.handlers[symbol].on_book_ticker.process_line(data)

    def on_close(self, ws, code, message):

        for sym, handler in self.handlers.values():

            print(f"Flushing {sym} data...")

            handler.on_agg_trade.handle.flush()
            handler.on_depth20.handle.flush()
            handler.on_force_order.handle.flush()
            handler.on_kline_1m.handle.flush()
            handler.on_book_ticker.handle.flush()


if __name__ == '__main__':

    symbols_all = ["ethusdt", "btcusdt", "dogeusdt", "xrpusdt", "solusdt", "suiusdt"]

    s = Rec2CsvStrategy(
        symbols=symbols_all,
        parent_path="./raw_folder",
        proxy=[
            "http://127.0.0.1:7890",
            "http://i.**REMOVED**:7890",
        ],
        log_file="./raw_folder/log.txt",
        ws_trace=False
    )

    for symbol in symbols_all:
        s.subscribe(symbol, "aggTrade", write_to_log=True)
        s.subscribe(symbol, "kline_1m", write_to_log=True)
        s.subscribe(symbol, "depth20@100ms", write_to_log=True)
        s.subscribe(symbol, "forceOrder", write_to_log=True)
        s.subscribe(symbol, "bookTicker", write_to_log=True)

    s.run()
