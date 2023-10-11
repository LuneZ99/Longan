import multiprocessing
import os
import signal
import time
from logging import INFO, WARNING

from diskcache import Cache

from binance_md.bn_ws_client import BaseBinanceWSClient
from binance_md.data_handler import *
from binance_md.utils import logger_md


def split_list(lst, num_parts):
    result = [[] for _ in range(num_parts)]
    for ii, num in enumerate(lst):
        index = ii % num_parts
        result[index].append(num)
    return result


def signal_handler(signum, frame):
    logger_md.log(WARNING, "Close all workers.")

    interrupt_cache = Cache("/dev/shm/binance_md_interrupt")
    interrupt_cache['flag'] = True

    for ii, pp in enumerate(multiprocessing.active_children()):
        # p.terminate()
        logger_md.log(WARNING, f"Closing... subprocess {ii}")
        os.kill(pp.pid, signal.SIGINT)
        pp.join()

    # logger_md.log(WARN, "Clear cache folder.")
    # clear_cache_folder()

    logger_md.log(WARNING, f"Cache folder size {get_cache_folder_size()} M.")
    time.sleep(10)

    exit(0)


class SymbolStreamMysqlHandler:

    def __init__(self, symbol):
        self.on_kline_1m = KlineHandler(symbol, 'kline_1m')
        self.on_kline_1h = KlineHandler(symbol, 'kline_1h')
        self.on_kline_8h = KlineHandler(symbol, 'kline_8h')
        self.on_agg_trade = AggTradeHandler(symbol)
        self.on_book_ticker = BookTickerHandler(symbol)
        self.on_depth20 = Depth20Handler(symbol)

    def on_close(self):
        self.on_agg_trade.on_close()
        self.on_kline_1m.on_close()
        self.on_kline_1h.on_close()
        self.on_kline_8h.on_close()
        self.on_depth20.on_close()


class BinanceFutureMD(BaseBinanceWSClient):
    handlers: dict[str, SymbolStreamMysqlHandler]

    def __init__(self, name, symbols=None, proxy=None, ws_trace=False, debug=False):
        super().__init__(name, proxy, ws_trace, debug)

        self.handlers: dict[str, SymbolStreamMysqlHandler] = {
            symbol: SymbolStreamMysqlHandler(symbol)
            for symbol in symbols
        }

    def on_agg_trade(self, symbol: str, data: dict, rec_time: int):
        self.handlers[symbol].on_agg_trade.process_line(data, rec_time)

    def on_depth20(self, symbol: str, data: dict, rec_time: int):
        self.handlers[symbol].on_depth20.process_line(data, rec_time)

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
        self.handlers[symbol].on_book_ticker.process_line(data, rec_time)
        pass

    def on_close(self, ws, code, message):
        pass


def md2sql_worker(
        name, symbols_all, subscribe_list,
        log_interval=10000, proxy="http://127.0.0.1:7890", ws_trace=False
):
    s = BinanceFutureMD(
        name=name,
        symbols=symbols_all,
        proxy=proxy,
        ws_trace=ws_trace
    )

    for _symbol in symbols_all:
        for md in subscribe_list:
            s.subscribe(_symbol, md, log_interval=log_interval)

    s.run()


if __name__ == '__main__':

    from binance_md.utils import config

    signal.signal(signal.SIGINT, signal_handler)

    symbols = config.future_symbols

    subscribe_list_all = [
        'kline_1m',
        'kline_1h',
        'kline_8h',
        'aggTrade',
        'bookTicker',
        'depth20'
    ]

    split_num = len(symbols) * len(subscribe_list_all) // 100 + 1
    split_symbols = split_list(symbols, split_num)

    logger_md.log(INFO, f"Starting with {len(symbols)} symbols, split to {split_num} MD workers.")

    processes = list()
    for i, symbols in enumerate(split_symbols):
        p = multiprocessing.Process(
            target=md2sql_worker, args=(i, symbols, subscribe_list_all, 0, config.proxy_url, False)
        )
        p.daemon = True
        p.start()
        processes.append(p)

    while True:
        time.sleep(1)
