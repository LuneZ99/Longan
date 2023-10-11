from peewee import Model


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
        raise NotImplementedError

    def _process_line(self, data, rec_time) -> dict:
        raise NotImplementedError
