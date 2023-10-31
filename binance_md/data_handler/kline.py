from typing import Any

from peewee import *

from binance_md.data_handler.DiskCacheHandler import BaseStreamDiskCacheMysqlHandler
from binance_md.utils import config, generate_models

# 设置 MySQL 数据库连接
db = MySQLDatabase(
    'binance_kline',
    user=config.mysql.user,
    password=config.mysql.password,
    host=config.mysql.host,
    port=config.mysql.port
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

    finish = BooleanField(null=False)

    class Meta:
        database = db
        indexes = (
            (('symbol',), False),
            (('close_time',), False),
        )


models_kline: dict[str, BaseKline] = generate_models(config.kline_list, BaseKline)


class KlineHandler(BaseStreamDiskCacheMysqlHandler):
    # cross-section KLine in and sql

    def __init__(self, symbol, event, expire_time=32 * 24 * 60 * 60, flush_interval=120):
        super().__init__(symbol, event, expire_time, flush_interval)
        self.model = models_kline[self.event]

    def _process_line(self, data, rec_time) -> tuple[Any, dict]:
        return data['k']['t'], dict(
            symbol=self.symbol,
            rec_time=rec_time,
            event_time=data['E'],
            open_time=data['k']['t'],
            close_time=data['k']['T'],
            open=float(data['k']['o']),
            high=float(data['k']['h']),
            low=float(data['k']['l']),
            close=float(data['k']['c']),
            volume=float(data['k']['v']),
            quote_volume=float(data['k']['q']),
            count=data['k']['n'],
            taker_buy_volume=float(data['k']['V']),
            taker_buy_quote_volume=float(data['k']['Q']),
            finish=data['k']['x']
        )
