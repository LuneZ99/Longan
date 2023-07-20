import os

from Strategy import BinanceSyncStrategy
from Handler import SymbolStreamCsvHandler


class Rec2CsvStrategy(BinanceSyncStrategy):
    handlers: dict[str, SymbolStreamCsvHandler]

    def __init__(self, parent_path, log_file=None, symbols=None, config_file=None, proxy=None, ws_trace=False, debug=False):

        if log_file is None:
            log_file = os.path.join(parent_path, "log.txt")

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

    # symbols_all = ["ethusdt", "btcusdt", "dogeusdt", "xrpusdt", "solusdt", "suiusdt"]
    #
    # s = Rec2CsvStrategy(
    #     symbols=symbols_all,
    #     parent_path="./raw_folder",
    #     proxy=[
    #         "http://127.0.0.1:7890",
    #         "http://i.**REMOVED**:7890",
    #     ],
    #     ws_trace=False
    # )
    #
    # for _symbol in symbols_all:
    #     s.subscribe(_symbol, "aggTrade", write_to_log=True)
    #     s.subscribe(_symbol, "kline_1m", write_to_log=True)
    #     s.subscribe(_symbol, "depth20@100ms", write_to_log=True)
    #     s.subscribe(_symbol, "forceOrder", write_to_log=True)
    #     s.subscribe(_symbol, "bookTicker", write_to_log=True)
    #
    # s.run()

    import argparse

    # 创建 ArgumentParser 对象
    parser = argparse.ArgumentParser(description="Rec2CsvStrategy Command Line Interface")

    # 添加命令行参数
    parser.add_argument("--symbols", type=str, nargs="+",
                        default=["ethusdt", "btcusdt", "dogeusdt", "xrpusdt", "solusdt", "suiusdt"],
                        help="List of symbols to subscribe")
    parser.add_argument("--parent_path", type=str, default="./raw_folder", help="Parent directory path")
    parser.add_argument("--proxy", type=str, nargs="+",
                        default=["http://127.0.0.1:7890", "http://i.**REMOVED**:7890"],
                        help="List of proxy URLs")
    parser.add_argument("--log_file", type=str, default="./raw_folder/log.txt", help="Log file path")
    parser.add_argument("--ws_trace", action="store_true", help="Enable WebSocket trace")
    parser.add_argument("--subscriptions", type=str, nargs="+",
                        default=["aggTrade", "kline_1m", "depth20@100ms", "forceOrder", "bookTicker"],
                        help="List of subscription strings")

    # 解析命令行参数
    args = parser.parse_args()

    s = Rec2CsvStrategy(
        symbols=args.symbols,
        parent_path=args.parent_path,
        proxy=args.proxy,
        log_file=args.log_file,
        ws_trace=args.ws_trace
    )

    for _symbol in args.symbols:
        for subscription in args.subscriptions:
            s.subscribe(_symbol, subscription, write_to_log=True)

    s.run()

