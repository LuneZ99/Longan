from typing import Any

from peewee import *

from binance_md.future.data_handler.DiskCacheHandler import BaseStreamDiskCacheMysqlHandler
from binance_md.future.utils import config, generate_models

# 设置 MySQL 数据库连接
db = MySQLDatabase(
    'binance_future_agg_trade',
    user=config.mysql['user'],
    password=config.mysql['password'],
    host=config.mysql['host'],
    port=config.mysql['port']
)


class BaseAggTrade(Model):
    agg_trade_id = BigIntegerField(primary_key=True, null=False)
    rec_time = BigIntegerField(null=False)
    event_time = BigIntegerField(null=False)
    price = FloatField(null=False)
    quantity = FloatField(null=False)
    first_trade_id = BigIntegerField(null=False)
    last_trade_id = BigIntegerField(null=False)
    transact_time = BigIntegerField(null=False)
    is_buyer_maker = BooleanField(null=False)

    class Meta:
        database = db
        indexes = (
            (('agg_trade_id',), True),
        )


models_agg_trades = generate_models(config.future_symbols, BaseAggTrade)


class AggTradeHandler(BaseStreamDiskCacheMysqlHandler):

    def __init__(self, symbol, event='aggTrade', expire_time=180, flush_interval=120):
        super().__init__(symbol, event, expire_time, flush_interval)
        self.model = models_agg_trades[self.symbol]

    def _process_line(self, data, rec_time) -> tuple[Any, dict]:
        return data['T'], dict(
            agg_trade_id=data['a'],
            rec_time=rec_time,
            event_time=data['E'],
            price=float(data['p']),
            quantity=float(data['q']),
            first_trade_id=data['f'],
            last_trade_id=data['l'],
            transact_time=data['T'],
            is_buyer_maker=data['m']
        )
