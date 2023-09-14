import multiprocessing
import os
import signal
import time
from logging import WARN, INFO

from market_data.bn_ws_client import BaseBinanceWSClient
from market_data.logger import logger_md
from mysql_handler import *


def split_list(lst, num_parts):
    result = [[] for _ in range(num_parts)]
    for ii, num in enumerate(lst):
        index = ii % num_parts
        result[index].append(num)
    return result


def signal_handler(signum, frame):
    logger_md.log(WARN, "Close all workers.")
    for ii, pp in enumerate(multiprocessing.active_children()):
        # p.terminate()
        logger_md.log(WARN, f"Closing... subprocess {ii}")
        os.kill(pp.pid, signal.SIGINT)
        pp.join()

    # logger_md.log(WARN, "Clear cache folder.")
    # clear_cache_folder()

    logger_md.log(WARN, f"Cache folder size {get_cache_folder_size()} M.")
    time.sleep(5)

    exit(0)


class SymbolStreamMysqlHandler:

    def __init__(self, symbol):
        self.on_kline_1m = KlineHandler(symbol, 'kline_1m')
        self.on_kline_1h = KlineHandler(symbol, 'kline_1h')
        self.on_kline_8h = KlineHandler(symbol, 'kline_8h')
        self.on_agg_trade = AggTradeHandler(symbol)
        self.on_book_ticker = BookTickerHandler(symbol)

    def on_close(self):
        self.on_agg_trade.on_close()
        self.on_kline_1m.on_close()
        self.on_kline_1h.on_close()
        self.on_kline_8h.on_close()


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

    signal.signal(signal.SIGINT, signal_handler)

    symbols = [f'{x}USDT'.lower() for x in [
        '1000FLOKI', '1000LUNC', '1000PEPE', '1000SHIB', '1000XEC', '1INCH', 'AAVE', 'ACH', 'ADA', 'AGIX', 'AGLD',
        'ALGO', 'ALICE', 'ALPHA', 'AMB', 'ANKR', 'ANT', 'APE', 'API3', 'APT', 'ARB', 'ARKM', 'ARPA', 'AR', 'ASTR',
        'ATA', 'ATOM', 'AUDIO', 'AVAX', 'AXS', 'BAKE', 'BAL', 'BAND', 'BAT', 'BCH', 'BEL', 'BLUEBIRD', 'BLUR', 'BLZ',
        'BNB', 'BNT', 'BNX', 'BTCDOM', 'BTC', 'C98', 'CELO', 'CELR', 'CFX', 'CHR', 'CHZ', 'CKB', 'COMBO', 'COMP',
        'COTI', 'CRV', 'CTK', 'CTSI', 'CVX', 'CYBER', 'DAR', 'DASH', 'DEFI', 'DENT', 'DGB', 'DODOX', 'DOGE', 'DOT',
        'DUSK', 'DYDX', 'EDU', 'EGLD', 'ENJ', 'ENS', 'ETC', 'ETH', 'FET', 'FIL', 'FLOW', 'FOOTBALL', 'FTM', 'FXS',
        'GALA', 'GAL', 'GMT', 'GMX', 'GRT', 'GTC', 'HBAR', 'HFT', 'HIGH', 'HOOK', 'HOT', 'ICP', 'ICX', 'IDEX', 'ID',
        'IMX', 'INJ', 'IOST', 'IOTA', 'IOTX', 'JASMY', 'JOE', 'KAVA', 'KEY', 'KLAY', 'KNC', 'KSM', 'LDO', 'LEVER',
        'LINA', 'LINK', 'LITU', 'LPTU', 'LQTY', 'LRC', 'LTC', 'LUNA2', 'MAGIC', 'MAMA', 'MASK', 'MATIC', 'MAV',
        'MDT', 'MINA', 'MKR', 'MTL', 'NEAR', 'NEO', 'NKN', 'NMR', 'OCEAN', 'OGN', 'OMG', 'ONE', 'ONT', 'OP', 'OXT',
        'PENDLE', 'PEOPLE', 'PERP', 'PHB', 'QNT', 'QTUM', 'RAD', 'RDNT', 'REEF', 'REN', 'RLC', 'RNDR', 'ROSE',
        'RSR', 'RUNE', 'RVN', 'SAND', 'SEI', 'SFP', 'SKL', 'SNX', 'SOL', 'SPELL', 'SSV', 'STG', 'STMX', 'STORJ', 'STX',
        'SUI', 'SUSHI', 'SXP', 'THETA', 'TLM', 'TOMO', 'TRB', 'TRU', 'TRX', 'T', 'UMA', 'UNFI', 'UNI', 'VET', 'WAVES',
        'WLD', 'WOO', 'XEM', 'XMR', 'XLM', 'XRP', 'XTZ', 'XVG', 'XVS', 'YFI', 'YGG', 'ZEC', 'ZEN', 'ZIL', 'ZRX'
    ]]

    subscribe_list_all = [
        'kline_1m',
        'kline_1h',
        'kline_8h',
        'aggTrade',
        'bookTicker',
        # 'depth20'
    ]

    split_num = len(symbols) * len(subscribe_list_all) // 200 + 2
    split_symbols = split_list(symbols, split_num)

    logger_md.log(INFO, f"Starting with {len(symbols)} symbols, split to {split_num} MD workers.")

    processes = list()
    for i, symbols in enumerate(split_symbols):
        p = multiprocessing.Process(target=md2sql_worker, args=(i, symbols, subscribe_list_all, 0))
        p.daemon = True
        p.start()
        processes.append(p)

    while True:
        time.sleep(1)
