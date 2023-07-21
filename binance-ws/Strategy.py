import json
import os
import sys
import time
import itertools

from collections import defaultdict
from datetime import datetime
from logging import INFO, ERROR
from pprint import pformat
from typing import Callable

import websocket

from Config import BinanceConfig
from Logger import BinanceSyncLogger


def format_dict(default_dict):
    if isinstance(default_dict, defaultdict):
        return {k: format_dict(v) for k, v in default_dict.items()}
    else:
        return default_dict.__str__().split(' ')[2]


class BinanceSyncStrategy:

    def __init__(self, log_file="log.default", config_file=None, proxy=None, ws_trace=False, debug=False):

        self.config = BinanceConfig()
        if config_file is not None:
            self.config = self.config.load_from_yaml(config_file)

        self.logger = BinanceSyncLogger(log_file)

        if proxy is not None:
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
            self.proxy = None

        self.symbols = set()
        self.handlers = dict()
        self.callbacks: dict[str, dict[str, Callable]] = defaultdict(lambda: defaultdict(lambda: self.on_missing))
        self.log_ctrl: dict[str, dict[str, bool]] = defaultdict(lambda: defaultdict(lambda: False))

        self.subscribe_url = "wss://fstream.binance.com/stream?streams="
        websocket.enableTrace(ws_trace)
        self.debug = debug

        self.connect_time = None
        self.connect_count = 0

    def subscribe(self, symbol: str, channel: str, write_to_log: bool = True):

        """
        订阅指定的symbol和channel
        """

        print(f"Subscribe {symbol}@{channel}")

        self.subscribe_url += f"{symbol}@{channel}/"
        self.symbols.add(symbol)

        if "@" in channel:
            channel = channel.split("@")[0]

        self.log_ctrl[symbol][channel] = write_to_log
        self.callbacks[symbol][channel] = self._get_callbacks(channel)

    def _get_callbacks(self, channel):

        converted_string = ""

        if "_" in channel:
            # 输入为下划线格式的字符串
            converted_string = "on_" + channel
        else:
            # 输入为驼峰格式的字符串
            for i, char in enumerate(channel):
                if i > 0 and char.isupper():
                    converted_string += "_"
                converted_string += char.lower()

            converted_string = "on_" + converted_string

        return self.__getattribute__(converted_string)

    def run(self):

        if not self.subscribe_url.endswith('/'):
            raise ValueError("Please subscribe a symbol first.")

        ws = websocket.WebSocketApp(
            self.subscribe_url,
            on_message=self._on_message,
            on_open=self._on_open,
            on_close=self._on_close
        )

        self.logger.log(INFO, f"Strategy Start with subscription url: {self.subscribe_url}")
        self.logger.log(INFO, f"CallBacks: \n{pformat(format_dict(self.callbacks))}")

        if self.proxy is None:
            while True:
                ws.run_forever()
                self.logger.log(ERROR, f"Websocket disconnected, retrying ...")
        else:
            for proxy in itertools.cycle(self.proxy):
                self.logger.log(INFO, f"Using proxy: {proxy}")
                ws.run_forever(
                    http_proxy_host=proxy[1],
                    http_proxy_port=proxy[2],
                    proxy_type=proxy[0]
                )
                self.logger.log(ERROR, f"Websocket disconnected, retrying ...")

    def _on_open(self, ws):
        self.connect_time = datetime.now()
        self.connect_count += 1
        self.logger.log(INFO, "Connection started.")

    def _on_close(self, ws, code, message):

        self.on_close(ws, code, message)

        if (datetime.now() - self.connect_time).seconds > 60:

            self.logger.log(ERROR, f"Connection close. Code {code}. Message {message}")
            self.logger.log(ERROR, f"Connection reset time {self.connect_count}, last run total time is {(datetime.now() - self.connect_time).seconds} seconds.")
            # self.run()
        else:
            self.logger.log(ERROR, f"Connection reset too quickly, stop !!!")
            sys.exit(0)

    def _on_message(self, ws, message):

        message = json.loads(message)
        rec_time = time.time_ns() // 1_000_000
        ori_time = message['data']['E']
        message['rec_time'] = rec_time
        message['delay'] = rec_time - ori_time
        stream_list = message['stream'].split('@')
        symbol = stream_list[0]
        event = stream_list[1]
        data: dict = message['data']

        if self.log_ctrl[symbol][event]:
            self.logger.log(INFO, message)

        self.callbacks[symbol][event](symbol, event, data, rec_time)

    @staticmethod
    def on_missing(symbol: str, name: str, data: dict, rec_time: int):
        raise KeyError(f"Missing Callback on {symbol} {name}")

    def on_agg_trade(self, symbol: str, name: str, data: dict, rec_time: int):
        raise NotImplementedError

    def on_depth20(self, symbol: str, name: str, data: dict, rec_time: int):
        raise NotImplementedError

    def on_force_order(self, symbol: str, name: str, data: dict, rec_time: int):
        raise NotImplementedError

    def on_kline_1m(self, symbol: str, name: str, data: dict, rec_time: int):
        raise NotImplementedError

    def on_book_ticker(self, symbol: str, name: str, data: dict, rec_time: int):
        raise NotImplementedError

    def on_close(self, ws, code, message):
        pass
