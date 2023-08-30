from peewee import *

from .BaseHandler import kline_list, generate_models, BaseStreamMysqlHandler

# 设置 MySQL 数据库连接
db = MySQLDatabase(
    'binance_kline',
    user='root',
    password='**REMOVED**',
    host='**REMOVED**',
    port=10003
)


class BaseKline(Model):
    id = AutoField(primary_key=True, null=False)

    symbol = CharField(null=False)
    rec_time = BigIntegerField(null=False)
    event_time = BigIntegerField(null=False)

    open_time = BigIntegerField(null=False)
    close_time = BigIntegerField(null=False)

    open = FloatField(null=False)
    high = FloatField(null=False)
    low = FloatField(null=False)
    close = FloatField(null=False)
    volume = FloatField(null=False)
    quote_volume = FloatField(null=False)
    count = BigIntegerField(null=False)
    taker_buy_volume = FloatField(null=False)
    taker_buy_quote_volume = FloatField(null=False)

    class Meta:
        database = db
        indexes = (
            (('symbol',), False),
            (('close_time',), False),
        )


models_kline: dict[str, BaseKline] = generate_models(kline_list, BaseKline)


class KlineHandler(BaseStreamMysqlHandler):

    def __init__(self, symbol, event):
        assert "kline" in event, "This handler is only supported for kline streams"
        super().__init__(symbol, event)

    def _process_line(self, data, rec_time):
        if data['k']['x']:
            models_kline[self.event.replace("-", "")].create(
                symbol=self.symbol,
                rec_time=rec_time,
                event_time=data['E'],
                open_time=data['k']['t'],
                close_time=data['k']['T'],
                open=data['k']['o'],
                high=data['k']['h'],
                low=data['k']['l'],
                close=data['k']['c'],
                volume=data['k']['v'],
                quote_volume=data['k']['q'],
                count=data['k']['n'],
                taker_buy_volume=data['k']['V'],
                taker_buy_quote_volume=data['k']['Q']
            )
