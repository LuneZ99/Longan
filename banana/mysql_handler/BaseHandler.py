# from gevent import monkey; monkey.patch_all()
# import gevent

import random
from datetime import datetime
from logging import INFO, WARN

from diskcache import Cache
from peewee import Model

from banana.market_feed.logger import logger_md

future_usdt_symbol_all = [f'{x}USDT'.lower() for x in [
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

kline_list = [
    'kline1m',
    'kline1h',
    'kline8h',
]

cache_folder = "/tmp/binance_cache"


class BaseHandler:
    date: str
    symbol: str
    event: str

    def on_close(self):
        raise NotImplementedError

    def process_line(self, data, rec_time):
        raise NotImplementedError

    def _process_line(self, data, rec_time) -> dict:
        raise NotImplementedError


class BaseStreamMysqlHandler(BaseHandler):
    model: Model

    def __init__(self, symbol, event):
        self.symbol = symbol
        self.event = event

    def on_close(self):
        pass

    def process_line(self, data, rec_time):
        if line := self._process_line(data, rec_time):
            self.model.create(**line)

    def _process_line(self, data, rec_time) -> dict:
        raise NotImplementedError


class BaseStreamDiskCacheHandler(BaseHandler):

    def __init__(self, symbol, event, expire_time):
        self.symbol = symbol
        self.event = event
        self.dc = Cache(f"{cache_folder}/{symbol}@{event}")
        logger_md.log(INFO, f"Create cache on {cache_folder}/{symbol}@{event}")
        self.expire_time = expire_time

    def on_close(self):
        pass

    def process_line(self, data, rec_time):
        if line := self._process_line(data, rec_time):
            self.dc.push(line, expire=self.expire_time)
            # logger_md.log(
            #     INFO,
            #     f"Push to {cache_folder}/{self.symbol}@{self.event}"
            # )

    def _process_line(self, data, rec_time) -> dict:
        raise NotImplementedError


class BaseStreamDiskCacheMysqlHandler(BaseHandler):
    model: Model

    def __init__(self, symbol, event, expire_time):
        self.symbol = symbol
        self.event = event
        self.dc = Cache(f"{cache_folder}/{symbol}@{event}")
        logger_md.log(INFO, f"Create cache on {cache_folder}/{symbol}@{event}")
        self.cache_list = list()
        self.expire_time = expire_time
        self.last_flush_time = datetime.now().minute
        self.rand_flush_second = random.randint(0, 58)

    def on_close(self):
        logger_md.log(WARN, f"Closing... Flush {self.symbol}@{self.event} to sql")
        self.flush_to_sql()

    def process_line(self, data, rec_time):
        if line := self._process_line(data, rec_time):
            self.dc.push(line, expire=self.expire_time)

            # logger_md.log(
            #     INFO,
            #     f"Push to {cache_folder}/{self.symbol}@{self.event}"
            # )

            self.cache_list.append(line)
            minute_now = datetime.now().minute
            second_now = datetime.now().second
            if (
                    (minute_now != self.last_flush_time and second_now > self.rand_flush_second) or
                    abs(minute_now - self.last_flush_time) > 1
            ) and len(self.cache_list) > 0:

                logger_md.log(
                    INFO,
                    f"Flush {self.symbol}@{self.event} [{len(self.cache_list)}] to sql, "
                    f"time_now: {minute_now:0>2}:{second_now:0>2}, last_flush_minute: {self.last_flush_time:0>2}."
                )
                self.flush_to_sql()
                # gevent.spawn(self.flush_to_sql).join()
                self.cache_list.clear()
                self.last_flush_time = minute_now

    def flush_to_sql(self):
        # with db.atomic():
        self.model.insert_many(self.cache_list).execute()

    def _process_line(self, data, rec_time) -> dict:
        raise NotImplementedError


def generate_models(table_names, meta_type):
    models = {}
    for table_name in table_names:
        # 动态创建Model类
        model_name = table_name.capitalize()
        model_meta = type('Meta', (object,), {'table_name': table_name})
        model_attrs = {
            'Meta': model_meta,
            '__module__': __name__
        }
        model: Model | meta_type | type = type(model_name, (meta_type,), model_attrs)
        models[table_name] = model

        if not model.table_exists():
            logger_md.log(
                WARN,
                f"{model} not exists, try create {table_name} in {model._meta.database.database}"
            )
            model.create_table()

    return models
