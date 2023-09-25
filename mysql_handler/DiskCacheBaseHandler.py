import os
import random
import shutil
import threading
from datetime import datetime, timedelta
from logging import INFO, WARN, DEBUG
from typing import Any

from diskcache import Cache
from peewee import Model

from market_data.logger import logger_md
from mysql_handler.BaseHandler import BaseHandler

cache_folder = "/mnt/0/tmp/binance_cache"


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


class BaseStreamDiskCacheHandler(BaseHandler):

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
