from typing import Any

from peewee import *

from binance_md.future.data_handler.DiskCacheHandler import BaseStreamDiskCacheMysqlHandler
from binance_md.future.utils import config, generate_models

# 设置 MySQL 数据库连接
db = MySQLDatabase(
    'binance_future_depth20',
    user=config.mysql['user'],
    password=config.mysql['password'],
    host=config.mysql['host'],
    port=config.mysql['port']
)


class BaseDepth20(Model):
    update_id = BigIntegerField(primary_key=True, null=False)
    prev_update_id = BigIntegerField(null=False)
    rec_time = BigIntegerField(null=False)
    event_time = BigIntegerField(null=False)
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
            (('update_id',), True),
        )


models_depth = generate_models(config.future_symbols, BaseDepth20)
cols_bp = [f"bp{i}" for i in range(1, 21)]
cols_bv = [f"bv{i}" for i in range(1, 21)]
cols_sp = [f"sp{i}" for i in range(1, 21)]
cols_sv = [f"sv{i}" for i in range(1, 21)]


class Depth20Handler(BaseStreamDiskCacheMysqlHandler):

    def __init__(self, symbol, event='depth20', expire_time=300, flush_interval=120):
        super().__init__(symbol, event, expire_time, flush_interval)
        self.model = models_depth[self.symbol]
        self.last_data = None

    def _process_line(self, data, rec_time) -> tuple[Any, dict]:

        dic_t = dict(
            update_id=data['u'],
            prev_update_id=data['pu'],
            rec_time=rec_time,
            event_time=data['E'],
            trade_time=data['T'],
        )

        dic_d = dict()

        for i, pv in enumerate(data['b']):
            dic_d[cols_bp[i]] = float(pv[0])
            dic_d[cols_bv[i]] = float(pv[1])

        for i, pv in enumerate(data['a']):
            dic_d[cols_sp[i]] = float(pv[0])
            dic_d[cols_sv[i]] = float(pv[1])

        if dic_d == self.last_data:
            return None, dict()

        self.last_data = dic_d

        return data['T'], {
            **dic_t,
            **dic_d
        }
