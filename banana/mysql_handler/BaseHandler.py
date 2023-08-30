# from gevent import monkey; monkey.patch_all()
# import gevent
import os
import random
import shutil
import threading
from datetime import datetime, timedelta
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


def clear_cache_folder():
    shutil.rmtree(cache_folder)


def get_cache_folder_size():
    total_size = 0
    for path, dirs, files in os.walk(cache_folder):
        for file in files:
            file_path = os.path.join(path, file)
            total_size += os.path.getsize(file_path)

    # MB
    return round(total_size / 1024 / 1024, 2)


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
        cache_path = f"{cache_folder}/{symbol}@{event}"
        if not os.path.exists(cache_path):
            logger_md.log(INFO, f"Create cache on {cache_folder}/{symbol}@{event}")
        self.dc = Cache(cache_path)
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
    timer: threading.Timer

    def __init__(self, symbol, event, expire_time):
        self.symbol = symbol
        self.event = event
        cache_path = f"{cache_folder}/{symbol}@{event}"
        if not os.path.exists(cache_path):
            logger_md.log(INFO, f"Create cache on {cache_folder}/{symbol}@{event}")
        self.dc = Cache(cache_path)
        self.cache_list = list()
        self.expire_time = expire_time
        self.flush_second = random.randint(5, 9) if "kline" in self.event else random.randint(0, 59)
        self.start_timer()

    def on_close(self):
        logger_md.log(WARN, f"Closing... Flush {self.symbol}@{self.event} to sql")
        self.flush_to_sql()

    def process_line(self, data, rec_time):
        if line := self._process_line(data, rec_time):
            self.dc.push(line, expire=self.expire_time)
            self.cache_list.append(line)

    def flush_to_sql(self):
        # with db.atomic():
        if len(self.cache_list) > 0:
            logger_md.log(INFO, f"Flush {self.symbol}@{self.event} [{len(self.cache_list)}] to sql.")
            self.model.insert_many(self.cache_list).execute()
            self.cache_list.clear()

    def start_timer(self):

        now = datetime.now()
        next_run_time = now.replace(second=self.flush_second) + timedelta(minutes=1)
        time_diff = (next_run_time - now).total_seconds()

        self.timer = threading.Timer(time_diff, self.run_periodically)
        self.timer.start()

    def stop_timer(self):
        if self.timer:
            self.timer.cancel()

    def run_periodically(self):
        # 在这里执行您想要定时运行的操作
        self.flush_to_sql()

        # 计算下一次运行的时间
        now = datetime.now()
        next_run_time = now.replace(second=self.flush_second) + timedelta(minutes=1)
        # logger_md.log(INFO, f"{self.symbol}@{self.event} Now {now} Next {next_run_time}")
        time_diff = (next_run_time - now).total_seconds()
        # 重新启动定时器
        self.timer = threading.Timer(time_diff, self.run_periodically)
        self.timer.start()

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
