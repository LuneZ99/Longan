from peewee import *

# 设置 MySQL 数据库连接
db = MySQLDatabase(
    'binance',
    user='root',
    password='**REMOVED**',
    host='**REMOVED**',
    port=10003
)


class Kline1m(Model):
    id = AutoField(primary_key=True)

    symbol = CharField()
    rec_time = IntegerField()

    open_time = IntegerField()
    close_time = IntegerField()

    open = FloatField()
    high = FloatField()
    low = FloatField()
    close = FloatField()
    volume = FloatField()
    quote_volume = FloatField()
    count = IntegerField()
    taker_buy_volume = FloatField()
    taker_buy_quote_volume = FloatField()

    class Meta:
        database = db


class Kline8h(Model):
    id = AutoField(primary_key=True)

    symbol = CharField()
    rec_time = IntegerField()

    open_time = IntegerField()
    close_time = IntegerField()

    open = FloatField()
    high = FloatField()
    low = FloatField()
    close = FloatField()
    volume = FloatField()
    quote_volume = FloatField()
    count = IntegerField()
    taker_buy_volume = FloatField()
    taker_buy_quote_volume = FloatField()

    class Meta:
        database = db



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
        # print("xxxx")
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
        self.on_kline_1m = KlineHandler(path, symbol, 'kline_1m')
        self.on_book_ticker = BookTickerHandler(path, symbol)
        self.on_force_order = ForceOrderHandler(path, symbol)
        self.on_depth20 = Depth20Handler(path, symbol)
