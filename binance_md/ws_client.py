import json
from collections import defaultdict
from logging import INFO, ERROR, WARN
from pprint import pformat
from typing import Callable

import websocket
from diskcache import Cache

from binance_md.utils import config, logger_md
from tools import *


def format_dict(default_dict):
    if isinstance(default_dict, defaultdict):
        return {k: format_dict(v) for k, v in default_dict.items()}
    else:
        return default_dict.__str__().split(' ')[2]


class BaseBinanceWSClient:
    ws: websocket.WebSocketApp

    def __init__(self, name, proxy=None, ws_trace=False, debug=False):

        self.config = config
        self.logger = logger_md
        self.name = name

        if isinstance(proxy, str):
            self.proxy = [proxy.replace("/", "").split(':')]
            assert len(self.proxy[0]) == 3, "Invalid proxy format, use format like 'http://127.0.0.1:8888'"
        elif isinstance(proxy, list):
            self.proxy = []
            for p in proxy:
                _p = p.replace("/", "").split(':')
                self.proxy.append(_p)
                assert len(_p) == 3, "Invalid proxy format, use format like 'http://127.0.0.1:8888'"
        else:
            self.proxy = [[None for _ in range(3)]]

        self.symbols = set()
        self.handlers = dict()
        self.callbacks: dict[str, dict[str, Callable]] = defaultdict(lambda: defaultdict(lambda: self.on_missing))
        self.log_interval: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(lambda: 0))
        self.log_count: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(lambda: 0))

        self.total_message_count = 0

        self.subscribe_url = "wss://fstream.binance.com/stream?streams="
        self.subscribe_count = 0

        websocket.enableTrace(ws_trace)
        self.debug = debug

        self.connect_time = None
        self.connect_count = 0

        self.delay_warning_threshold = 1000
        self.delay_warning_last = 0
        self.delay_warning_interval = 60
        self.delay_warning_count = 0

        self.interrupt_cache = Cache(f"{config.cache_folder}/binance_md_interrupt")

        if config.push_to_litchi:
            try:
                self.litchi_md = websocket.create_connection(config.litchi_md_url)
                self.litchi_md.send(f"{MsgType.register}{RegisterType.sender}")
                self.log(INFO, "litchi_md connected")
            except ConnectionRefusedError:
                self.log(WARN, "ConnectionRefusedError, is litchi_md server running?")
                self.litchi_md = None
        else:
            self.litchi_md = None

    def subscribe(self, symbol: str, event: str, log_interval: int = True):

        """
        订阅指定的 symbol 和 event
        """

        self.log(INFO, f"Subscribe {symbol}@{event}")
        self.subscribe_url += f"{symbol}@{event}/"
        self.symbols.add(symbol)

        if "@" in event:
            event = event.split("@")[0]

        self.log_interval[symbol][event] = log_interval
        self.log_count[symbol][event] = 0
        self.callbacks[symbol][event] = self._get_callbacks(event)

        self.subscribe_count += 1
        if self.subscribe_count >= 200:
            self.log(ERROR, f"Subscribed {symbol} failed because {self.subscribe_count} is larger than 200")
            raise ValueError(f"Subscribed {symbol} failed because {self.subscribe_count} is larger than 200")

    def run(self, proxy):

        if not self.subscribe_url.endswith('/'):
            raise ValueError("Please subscribe a symbol first.")

        self.ws = websocket.WebSocketApp(
            self.subscribe_url[:-1],
            on_message=self._on_message,
            on_open=self._on_open,
            on_close=self._on_close
        )

        self.log(INFO, f"Strategy Start with subscription url: {self.subscribe_url[:-1]}")
        self.log(INFO, f"CallBacks: \n{pformat(format_dict(self.callbacks))}")
        self.log(INFO, f"Total subscribe num: {self.subscribe_count}")
        self.log(INFO, f"Using proxy: {proxy}, interrupt flag {self.interrupt_cache['flag']}")

        self.ws.run_forever(
            http_proxy_host=proxy[1],
            http_proxy_port=proxy[2],
            proxy_type=proxy[0],
            skip_utf8_validation=True
        )

    def _on_open(self, ws):
        self.connect_time = datetime.now()
        self.connect_count += 1
        self.log(INFO, f"Connection started.")

    def _on_message(self, ws, message):

        if self.interrupt_cache['flag']:
            # self.log(INFO, f"Msg after close")
            self.ws.close()
            return

        message = json.loads(message)
        self.total_message_count += 1
        rec_time = time.time_ns() // 1_000_000
        ori_time = message['data']['E']
        message['rec_time'] = rec_time
        message['delay'] = rec_time - ori_time
        stream_list = message['stream'].split('@')
        symbol = stream_list[0]
        event = stream_list[1]
        data: dict = message['data']

        self.log_count[symbol][event] += 1

        if self.log_interval[symbol][event] > 0 and \
                self.log_count[symbol][event] % self.log_interval[symbol][event] == 0:
            self.log(INFO, message)

        if message['delay'] > self.delay_warning_threshold:
            self.delay_warning_count += 1

        if message['delay'] > self.delay_warning_threshold and \
                time.time() - self.delay_warning_last > self.delay_warning_interval:
            self.log(
                WARN,
                f"Receiving {message['stream']} delay {message['delay']} ms. "
                f"{self.delay_warning_count} delay / {self.total_message_count} messages "
                f"({self.delay_warning_count / self.total_message_count:.2%})"
            )
            self.delay_warning_last = time.time()

        processed_data = self.callbacks[symbol][event](symbol, data, rec_time)

        # send to md
        if self.litchi_md is not None:
            try:
                if event in config.event_push_to_litchi_md:
                    processed_msg = {
                        "symbol": symbol,
                        "event": event,
                        "rec_time": rec_time,
                        "data": processed_data,
                    }
                    self.litchi_md.send(f"{MsgType.broadcast}{json.dumps(processed_msg)}")
            except ConnectionError:
                self.log(WARN, "Push to litchi_md ConnectionError, is litchi_md server running?")
                self.litchi_md = websocket.create_connection(config.litchi_md_url)
                self.litchi_md.send(f"{MsgType.register}{RegisterType.sender}")

    def _on_close(self, ws, code, message):
        for handler in self.handlers.values():
            handler.on_close()
        if self.litchi_md is not None:
            self.litchi_md.close()
        self.on_close()
        self.log(WARN, "Websocket Connection closing ...")

    @staticmethod
    def on_missing(symbol: str, name: str, data: dict, rec_time: int):
        raise KeyError(f"Missing Callback on {symbol} {name}")

    # def on_agg_trade(self, symbol: str, name: str, data: dict, rec_time: int):
    #     raise NotImplementedError
    #
    # def on_depth20(self, symbol: str, name: str, data: dict, rec_time: int):
    #     raise NotImplementedError
    #
    # def on_force_order(self, symbol: str, name: str, data: dict, rec_time: int):
    #     raise NotImplementedError
    #
    # def on_kline_1m(self, symbol: str, name: str, data: dict, rec_time: int):
    #     raise NotImplementedError
    #
    # def on_book_ticker(self, symbol: str, name: str, data: dict, rec_time: int):
    #     raise NotImplementedError

    def on_close(self):
        raise NotImplementedError

    def log(self, level, msg):
        self.logger.log(level, f"MD-{self.name:0>2}: {msg}")

    def _get_callbacks(self, event):

        converted_string = ""

        if "_" in event:
            # 输入为下划线格式的字符串
            converted_string = "on_" + event
        else:
            # 输入为驼峰格式的字符串
            for i, char in enumerate(event):
                if i > 0 and char.isupper():
                    converted_string += "_"
                converted_string += char.lower()

            converted_string = "on_" + converted_string

        return self.__getattribute__(converted_string)
