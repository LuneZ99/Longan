import itertools
import multiprocessing
import signal
from logging import INFO, WARNING

from diskcache import Cache

from binance_md.data_handler import *
from binance_md.utils import logger_md, config
from binance_md.ws_client import BaseBinanceWSClient
from tools import *


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
        self.on_book_ticker.on_close()
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
        return self.handlers[symbol].on_agg_trade.process_line(data, rec_time)

    def on_depth20(self, symbol: str, data: dict, rec_time: int):
        return self.handlers[symbol].on_depth20.process_line(data, rec_time)

    def on_force_order(self, symbol: str, data: dict, rec_time: int):
        pass

    def on_kline_1m(self, symbol: str, data: dict, rec_time: int):
        return self.handlers[symbol].on_kline_1m.process_line(data, rec_time)

    def on_kline_8h(self, symbol: str, data: dict, rec_time: int):
        return self.handlers[symbol].on_kline_8h.process_line(data, rec_time)

    def on_kline_1h(self, symbol: str, data: dict, rec_time: int):
        return self.handlers[symbol].on_kline_1h.process_line(data, rec_time)

    def on_book_ticker(self, symbol: str, data: dict, rec_time: int):
        return self.handlers[symbol].on_book_ticker.process_line(data, rec_time)

    def on_close(self):
        pass


def binance_md_ws_worker(
        name, symbols_all, subscribe_list,
        log_interval=10000, proxy="http://127.0.0.1:7890", ws_trace=False
):
    worker = BinanceFutureMD(
        name=name,
        symbols=symbols_all,
        proxy=proxy,
        ws_trace=ws_trace
    )

    _interrupt_cache = Cache(f"{config.cache_folder}/binance_md_interrupt")

    for _symbol in symbols_all:
        for event in subscribe_list:
            worker.subscribe(_symbol, event, log_interval=log_interval)

    for proxy in itertools.cycle(worker.proxy):
        if not _interrupt_cache['flag']:
            logger_md.log(WARNING, f"MD-{name:0>2}: Connection lost retrying...")
            worker.run(proxy)
        else:
            logger_md.log(WARNING, f"MD-{name:0>2}: Receiving interrupt_cache signal, closing all workers.")
            break


interrupt_cache = Cache(f"{config.cache_folder}/binance_md_interrupt")
interrupt_cache.clear()
interrupt_cache['flag'] = False


def signal_handler(signum, frame):
    global interrupt_cache
    logger_md.log(WARNING, "Send interrupt_cache signal, closing all workers.")
    interrupt_cache['flag'] = True


signal.signal(signal.SIGINT, signal_handler)

split_num = len(config.future_symbols) * len(
    config.subscribe_events) // 100 + 1 if config.num_threads == 0 else config.num_threads
logger_md.log(INFO, f"Starting with {len(config.future_symbols)} symbols, split to {split_num} MD workers.")

for i, _symbols in enumerate(split_list_averagely(config.future_symbols, split_num)):
    p = multiprocessing.Process(
        target=binance_md_ws_worker, args=(i, _symbols, config.subscribe_events, 0, config.proxy_url, False)
    )
    p.daemon = True
    p.start()

while not interrupt_cache['flag']:
    time.sleep(1)
else:
    while not all(interrupt_cache[k] for k in interrupt_cache.iterkeys()):
        logger_md.log(
            INFO,
            f"Waiting for remain "
            f"{[k for k in interrupt_cache.iterkeys() if interrupt_cache[k] is False]} "
            f"workers closing... "
        )
        time.sleep(1)
    else:
        logger_md.log(INFO, f"All workers closed.")
