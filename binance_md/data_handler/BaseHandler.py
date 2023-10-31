from diskcache import Cache
from peewee import Model

from binance_md.utils import config


class BaseHandler:
    date: str
    symbol: str
    event: str

    def __init__(self):
        self.interrupt_cache = Cache(f"{config.cache_folder}/binance_md_interrupt")
        self.interrupt_cache[f"{self.symbol}@{self.event}"] = False

    def on_close(self):
        self._on_close()
        self.interrupt_cache[f"{self.symbol}@{self.event}"] = True

    def _on_close(self):
        raise NotImplementedError

    def process_line(self, data, rec_time):
        raise NotImplementedError

    def _process_line(self, data, rec_time) -> dict:
        raise NotImplementedError


class BaseStreamMysqlHandler(BaseHandler):
    model: Model

    def __init__(self, symbol, event):
        super().__init__()
        self.symbol = symbol
        self.event = event

    def _on_close(self):
        pass

    def process_line(self, data, rec_time):
        raise NotImplementedError

    def _process_line(self, data, rec_time) -> dict:
        raise NotImplementedError
