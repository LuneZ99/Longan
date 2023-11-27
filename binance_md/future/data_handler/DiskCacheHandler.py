import os
import random
import threading
from datetime import datetime, timedelta
from logging import INFO, WARN, DEBUG
from typing import Any

from diskcache import Cache
from peewee import Model

from binance_md.future.data_handler.BaseHandler import BaseHandler
from binance_md.future.utils import logger
from tools import global_config

cache_folder = global_config.future_md_ws_cache


class BaseStreamDiskCacheHandler(BaseHandler):

    def __init__(self, symbol, event, expire_time):
        self.symbol = symbol
        self.event = event
        cache_path = f"{cache_folder}/{symbol}@{event}"
        if not os.path.exists(cache_path):
            logger.log(INFO, f"Create cache on {cache_folder}/{symbol}@{event}")
        self.dc = Cache(cache_path, timeout=0.5)
        self.expire_time = expire_time
        super().__init__()

    def _on_close(self):
        pass

    def process_line(self, data, rec_time):
        key, line = self._process_line(data, rec_time)
        lst = list(line.values())
        if key is None:
            self.dc.push(lst, expire=self.expire_time)
        else:
            self.dc.set(key, lst, expire=self.expire_time)
        return lst

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
            logger.log(INFO, f"Create cache on {cache_folder}/{symbol}@{event}")
        self.dc = Cache(cache_path, timeout=0.5)
        logger.log(DEBUG, f"Success load cache on {cache_folder}/{symbol}@{event}")
        self.cache_list = list()
        self.expire_time = expire_time
        self.flush_interval = flush_interval
        self.avg_delay = 0
        self.rec_count = 0
        self.last_delay = 0
        super().__init__()
        self.start_timer()

    def _on_close(self):
        logger.log(WARN, f"Closing... Flush {self.symbol}@{self.event} to sql")
        self.flush_to_sql()

    def process_line(self, data, rec_time):
        key, line = self._process_line(data, rec_time)

        if line == dict():
            return

        self.last_delay = line['rec_time'] - line['event_time']
        self.avg_delay = (self.avg_delay * self.rec_count + self.last_delay) / (self.rec_count + 1)
        self.rec_count += 1

        lst = list(line.values())

        if self.event.startswith('kline'):
            # monkey patch for k-line data
            # drop the symbol (str) column when set cache to improve performances
            lst = lst[1:]
            self.dc.set(key, lst, expire=self.expire_time)
            if line['finish']:
                self.cache_list.append(line)
        else:
            self.dc.set(key, lst, expire=self.expire_time)
            self.cache_list.append(line)

        self._process_line_callback()

        return lst

    def flush_to_sql(self):
        # with db.atomic():
        if len(self.cache_list) > 0:
            logger.log(
                INFO,
                f"Received {self.symbol:>13}@{self.event:<9} {self.rec_count:>10}[+{len(self.cache_list):>6}], "
                f"avg delay {self.avg_delay:>4.0f} ms, "
                f"last delay {self.last_delay:>4.0f} ms"
            )
            self.model.insert_many(self.cache_list).execute()
            self.cache_list.clear()

    def start_timer(self):
        self.timer = threading.Timer(
            self._get_time_diff() + random.uniform(0, self.flush_interval), self.run_periodically
        )
        self.timer.start()

    def stop_timer(self):
        if self.timer is not None:
            self.timer.cancel()

    def run_periodically(self):
        self.flush_to_sql()
        # logger.log(INFO, f"{self.symbol}@{self.event} Now {now} Next {next_run_time}")
        self.timer = threading.Timer(self._get_time_diff(), self.run_periodically)
        self.timer.start()

    def _get_time_diff(self):
        # 计算下一次运行的时间
        now = datetime.now()
        next_run_time = now + timedelta(seconds=self.flush_interval)

        # 避开分钟线更新
        if next_run_time.second <= 4 or next_run_time.second >= 59:
            next_run_time += timedelta(seconds=5)

        return (next_run_time - now).total_seconds()

    def _process_line(self, data, rec_time) -> tuple[Any, dict]:
        raise NotImplementedError

    def _process_line_callback(self):
        pass
