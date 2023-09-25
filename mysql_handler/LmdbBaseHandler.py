import os
import lmdb
import random
import shutil
import numpy as np
import threading
from datetime import datetime, timedelta
from logging import INFO, WARN, DEBUG
from typing import Any

from peewee import Model

from market_data.logger import logger_md
from mysql_handler.BaseHandler import BaseHandler

cache_folder = "/dev/shm/binance_cache"


class LmdbHandler:
    def __init__(self, arg_path, map_size=512 * 1024 * 1024, bwrite=True) -> None:
        self.path = arg_path
        self.map_size = map_size
        self.bw = bwrite
        self.b_status = self.bw
        self.env = lmdb.open(self.path, map_size=self.map_size, readonly=True, lock=False)

    def delete(self, k):
        if type(k) is str:
            k = k.encode()
        with self.env.begin(write=True) as txn:
            txn.delete(k)
            txn.commit()

    def put(self, k, v, timeout=None):
        if type(k) is str:
            k = k.encode()
        if type(v) is str:
            v = v.encode()
        with self.env.begin(write=True) as txn:
            txn.put(key=k, value=v)
            txn.commit()

    def get(self, k, col):
        if type(k) is str:
            k = k.encode()

        with self.env.begin(buffers=True) as txn:
            ret = txn.get(k)
            if ret is None:
                return None
            if col > 0:
                v = np.frombuffer(ret, dtype=np.int32)
                return v.reshape(-1, col)
            elif col == 0:
                # return v.decode('UTF-8')
                pass
        return None


class BaseStreamLmdbHandler(BaseHandler):

    def __init__(self, symbol, event, expire_time):
        self.symbol = symbol
        self.event = event
        cache_path = f"{cache_folder}/{symbol}@{event}"
        if not os.path.exists(cache_path):
            logger_md.log(INFO, f"Create cache on {cache_folder}/{symbol}@{event}")
        self.dc = Cache(cache_path, timeout=0.1)
        self.expire_time = expire_time

    def on_close(self):
        pass

    def process_line(self, data, rec_time):
        key, line = self._process_line(data, rec_time)
        if key is None:
            self.dc.push(line, expire=self.expire_time)
        else:
            self.dc.set(key, line, expire=self.expire_time)

    def _process_line(self, data, rec_time) -> tuple[str, dict]:
        raise NotImplementedError


class BaseStreamDiskCacheMysqlHandler(BaseHandler):
    model: Model
    timer: threading.Timer

    def __init__(self, symbol, event, expire_time, flush_interval):
        self.symbol = symbol
        self.event = event
        cache_path = f"{cache_folder}/{symbol}@{event}"
        if not os.path.exists(cache_path):
            logger_md.log(INFO, f"Create cache on {cache_folder}/{symbol}@{event}")
        self.dc = Cache(cache_path, timeout=0.1)
        logger_md.log(DEBUG, f"Success load cache on {cache_folder}/{symbol}@{event}")
        self.cache_list = list()
        self.expire_time = expire_time
        self.flush_interval = flush_interval
        self.start_timer()

    def on_close(self):
        logger_md.log(WARN, f"Closing... Flush {self.symbol}@{self.event} to sql")
        self.flush_to_sql()

    def process_line(self, data, rec_time):
        key, line = self._process_line(data, rec_time)
        if key is None:
            self.dc.push(line, expire=self.expire_time)
        else:
            self.dc.set(key, line, expire=self.expire_time)
        self.cache_list.append(line)

    def flush_to_sql(self):
        # with db.atomic():
        if len(self.cache_list) > 0:
            logger_md.log(INFO, f"Flush {self.symbol}@{self.event} [{len(self.cache_list)}] to sql.")
            self.model.insert_many(self.cache_list).execute()
            self.cache_list.clear()

    def start_timer(self):

        self.timer = threading.Timer(self._get_time_diff() + random.uniform(0, self.flush_interval),
                                     self.run_periodically)
        self.timer.start()

    def stop_timer(self):
        if self.timer:
            self.timer.cancel()

    def run_periodically(self):
        # 在这里执行您想要定时运行的操作
        self.flush_to_sql()

        # logger_md.log(INFO, f"{self.symbol}@{self.event} Now {now} Next {next_run_time}")
        # 重新启动定时器
        self.timer = threading.Timer(self._get_time_diff(), self.run_periodically)
        self.timer.start()

    def _get_time_diff(self):
        # 计算下一次运行的时间
        now = datetime.now()
        next_run_time = now + timedelta(seconds=self.flush_interval)
        if next_run_time.second <= 3 or next_run_time.second >= 58:
            next_run_time += timedelta(seconds=5)

        return (next_run_time - now).total_seconds()

    def _process_line(self, data, rec_time) -> tuple[Any, dict]:
        raise NotImplementedError
