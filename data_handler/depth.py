from typing import Any

from peewee import *
from utils import config, generate_models
from data_handler.DiskCacheHandler import BaseStreamDiskCacheMysqlHandler


# 设置 MySQL 数据库连接
db = MySQLDatabase(
    'binance_depth',
    user=config.mysql.user,
    password=config.mysql.password,
    host=config.mysql.host,
    port=config.mysql.port
)


cols_b = [f"bp{i},bv{i}" for i in range(1, 21)]
cols_s = [f"sp{i},sv{i}" for i in range(1, 21)]


class BaseDepth20(Model):
    orig_time = BigIntegerField(primary_key=True, null=False)
    trade_time = BigIntegerField(null=False)

    bp1 = FloatField(null=False)
    bv1 = FloatField(null=False)
    bp2 = FloatField(null=False)
    bv2 = FloatField(null=False)
    bp3 = FloatField(null=False)
    bv3 = FloatField(null=False)
    bp4 = FloatField(null=False)
    bv4 = FloatField(null=False)
    bp5 = FloatField(null=False)
    bv5 = FloatField(null=False)
    bp6 = FloatField(null=False)
    bv6 = FloatField(null=False)
    bp7 = FloatField(null=False)
    bv7 = FloatField(null=False)
    bp8 = FloatField(null=False)
    bv8 = FloatField(null=False)
    bp9 = FloatField(null=False)
    bv9 = FloatField(null=False)
    bp10 = FloatField(null=False)
    bv10 = FloatField(null=False)

    bp11 = FloatField(null=False)
    bv11 = FloatField(null=False)
    bp12 = FloatField(null=False)
    bv12 = FloatField(null=False)
    bp13 = FloatField(null=False)
    bv13 = FloatField(null=False)
    bp14 = FloatField(null=False)
    bv14 = FloatField(null=False)
    bp15 = FloatField(null=False)
    bv15 = FloatField(null=False)
    bp16 = FloatField(null=False)
    bv16 = FloatField(null=False)
    bp17 = FloatField(null=False)
    bv17 = FloatField(null=False)
    bp18 = FloatField(null=False)
    bv18 = FloatField(null=False)
    bp19 = FloatField(null=False)
    bv19 = FloatField(null=False)
    bp20 = FloatField(null=False)
    bv20 = FloatField(null=False)

    sp1 = FloatField(null=False)
    sv1 = FloatField(null=False)
    sp2 = FloatField(null=False)
    sv2 = FloatField(null=False)
    sp3 = FloatField(null=False)
    sv3 = FloatField(null=False)
    sp4 = FloatField(null=False)
    sv4 = FloatField(null=False)
    sp5 = FloatField(null=False)
    sv5 = FloatField(null=False)
    sp6 = FloatField(null=False)
    sv6 = FloatField(null=False)
    sp7 = FloatField(null=False)
    sv7 = FloatField(null=False)
    sp8 = FloatField(null=False)
    sv8 = FloatField(null=False)
    sp9 = FloatField(null=False)
    sv9 = FloatField(null=False)
    sp10 = FloatField(null=False)
    sv10 = FloatField(null=False)

    sp11 = FloatField(null=False)
    sv11 = FloatField(null=False)
    sp12 = FloatField(null=False)
    sv12 = FloatField(null=False)
    sp13 = FloatField(null=False)
    sv13 = FloatField(null=False)
    sp14 = FloatField(null=False)
    sv14 = FloatField(null=False)
    sp15 = FloatField(null=False)
    sv15 = FloatField(null=False)
    sp16 = FloatField(null=False)
    sv16 = FloatField(null=False)
    sp17 = FloatField(null=False)
    sv17 = FloatField(null=False)
    sp18 = FloatField(null=False)
    sv18 = FloatField(null=False)
    sp19 = FloatField(null=False)
    sv19 = FloatField(null=False)
    sp20 = FloatField(null=False)
    sv20 = FloatField(null=False)

    class Meta:
        database = db
        indexes = (
            (('orig_time',), True),
        )


models_depth = generate_models(config.usdt_future_symbol_all, BaseDepth20)


class Depth20Handler(BaseStreamDiskCacheMysqlHandler):

    def __init__(self, symbol, event='depth', expire_time=180, flush_interval=120):
        super().__init__(symbol, event, expire_time, flush_interval)
        self.model = models_depth[self.symbol]
        self.last_data = None

    def _process_line(self, data, rec_time) -> tuple[Any, dict]:

        dic_t = dict(
            orig_time=data['E'],
            trade_time=data['T']
        )

        dic_d = dict()

        for i, pv in enumerate(data['b']):
            dic_d[f"bp{i + 1}"] = pv[0]
            dic_d[f"bv{i + 1}"] = pv[1]

        for i, pv in enumerate(data['a']):
            dic_d[f"sp{i + 1}"] = pv[0]
            dic_d[f"sv{i + 1}"] = pv[1]

        if dic_d == self.last_data:
            return None, dict()

        self.last_data = dic_d

        return data['T'], {
            **dic_t,
            **dic_d
        }
