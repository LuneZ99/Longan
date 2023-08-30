from peewee import *

from .BaseHandler import future_usdt_symbol_all, generate_models, BaseStreamMysqlHandler

# 设置 MySQL 数据库连接
db = MySQLDatabase(
    'binance_book_ticker',
    user='root',
    password='**REMOVED**',
    host='**REMOVED**',
    port=10003
)


class BaseBookTicker(Model):
    uid = BigIntegerField(primary_key=True, null=False)
    rec_time = BigIntegerField(null=False)
    event_time = BigIntegerField(null=False)
    transaction_time = BigIntegerField(null=False)

    best_bid_price = FloatField(null=False)
    best_bid_qty = FloatField(null=False)
    best_ask_price = FloatField(null=False)
    best_ask_qty = FloatField(null=False)

    class Meta:
        database = db
        indexes = (
            (('uid',), True),
        )


models_book_ticker = generate_models(future_usdt_symbol_all, BaseBookTicker)


class BookTickerHandler(BaseStreamMysqlHandler):

    def __init__(self, symbol, event='bookTicker'):
        super().__init__(symbol, event)

    def _process_line(self, data, rec_time):
        models_book_ticker[self.symbol].create(
            uid=data['u'],
            rec_time=rec_time,
            event_time=data['E'],
            transaction_time=data['T'],

            best_bid_price=data['b'],
            best_bid_qty=data['B'],
            best_ask_price=data['a'],
            best_ask_qty=data['A'],
        )
