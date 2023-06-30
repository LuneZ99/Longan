import os
from datetime import datetime
from typing import Optional, IO

import pytz


class BaseStreamCsvHandler:
    date: str
    symbol: str
    event: str
    handle: Optional[IO]
    headers: str
    file_name: str

    def __init__(self, path, symbol, event):
        self.parent_path = path
        self.symbol = symbol
        self.event = event
        self.handle = None

        now = datetime.now()
        utc_now = now.astimezone(pytz.utc)
        utc_date = str(utc_now)[:10]
        self.date = utc_date


    def on_close(self):
        if self.handle:
            self.handle.close()

    def process_line(self, info):
        now = datetime.now()
        utc_now = now.astimezone(pytz.utc)
        utc_date = str(utc_now)[:10]
        if self.date > utc_date:
            self.date = utc_date
            self._reset_handle()
        line = self._process_line(info)
        if len(line) > 0:
            self.handle.write("\n" + ",".join(map(str, line)))

    def _reset_handle(self):

        if self.handle:
            self.handle.close()

        self.file_name = f"{self.parent_path}/{self.symbol}/{self.date}/{self.event}.csv"

        if not os.path.exists(f"{self.parent_path}/{self.symbol}/{self.date}"):
            os.makedirs(f"{self.parent_path}/{self.symbol}/{self.date}")

        if os.path.exists(self.file_name):
            if os.path.getsize(self.file_name) <= 128:
                self.handle = open(self.file_name, "w")
                self._write_csv_header()
            else:
                self.handle = open(self.file_name, "a")
        else:
            self.handle = open(self.file_name, "w")
            self._write_csv_header()

    def _write_csv_header(self):
        self.handle.write(self.headers)

    def _process_line(self, info):
        raise NotImplementedError


class AggTradeHandler(BaseStreamCsvHandler):

    def __init__(self, path, symbol, event="aggTrade"):
        super().__init__(path, symbol, event)
        self.headers = "OrigTime,ID,Price,Volume,TradeFirst,TradeLast,TradeTime,isSell"
        self._reset_handle()

    def _process_line(self, info):
        return [
            info['E'],
            info['a'],
            info['p'],
            info['q'],
            info['f'],
            info['l'],
            info['T'],
            info['m']
        ]


class KlineHandler(BaseStreamCsvHandler):

    def __init__(self, path, symbol, stream):
        assert "kline" in stream, "This handler is only supported for kline streams"
        super().__init__(path, symbol, stream)
        self.headers = "OrigTime,TimeStart,TimeEnd,TradeFirst,TradeLast,Open,Close,High,Low,Volume,TradeCount,Money,BuyVolume,BuyMoney"
        self._reset_handle()

    def _process_line(self, info):

        if info['k']['x']:
            return [
                info['E'],
                info['k']['t'],
                info['k']['T'],
                info['k']['f'],
                info['k']['L'],
                info['k']['o'],
                info['k']['c'],
                info['k']['h'],
                info['k']['l'],
                info['k']['v'],
                info['k']['n'],
                info['k']['q'],
                info['k']['V'],
                info['k']['Q']
            ]

        else:
            return []


class BookTickerHandler(BaseStreamCsvHandler):

    def __init__(self, path, symbol, stream="bookTicker"):
        super().__init__(path, symbol, stream)
        self.headers = "OrigTime,ID,TradeTime,BP1,BV1,SP1,SV1"
        self._reset_handle()

    def _process_line(self, info):
        return [
            info['E'],
            info['u'],
            info['T'],
            info['b'],
            info['B'],
            info['a'],
            info['A']
        ]


class ForceOrderHandler(BaseStreamCsvHandler):

    def __init__(self, path, symbol, stream="forceOrder"):
        super().__init__(path, symbol, stream)
        self.headers = "OrigTime,OrderSide,OrderType,TimeInForce,Price,Volume,AvgPrice,OrderStatus,LastTradedVolume,TotalTradedVolume,TradeTime"
        self._reset_handle()

    def _process_line(self, info):
        return [
            info['E'],
            info['o']['S'],
            info['o']['o'],
            info['o']['f'],
            info['o']['p'],
            info['o']['q'],
            info['o']['ap'],
            info['o']['X'],
            info['o']['l'],
            info['o']['z'],
            info['o']['T']
        ]


class Depth20Handler(BaseStreamCsvHandler):

    # depth20@100ms

    def __init__(self, path, symbol, stream="depth"):
        super().__init__(path, symbol, stream)
        self.headers = "OrigTime,TradeTime," + ",".join(
            [f"BP{i},BV{i}" for i in range(1, 21)] + [f"SP{i},SV{i}" for i in range(1, 21)])
        self._reset_handle()

    def _process_line(self, info):
        return [info['E'], info['T']] + [f"{x[0]},{x[1]}" for x in info['b']] + [f"{x[0]},{x[1]}" for x in info['a']]


class SymbolStreamCsvHandler:

    def __init__(self, path, symbol):
        self.on_agg_trade = AggTradeHandler(path, symbol)
        # self.on_kline = KlineHandler(path, symbol)
        self.on_book_ticker = BookTickerHandler(path, symbol)
        self.on_force_order = ForceOrderHandler(path, symbol)
        self.on_depth20 = Depth20Handler(path, symbol)


