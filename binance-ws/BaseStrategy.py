import asyncio
import json
import os
from collections import defaultdict
from datetime import datetime
from logging import INFO
from pprint import pformat
from typing import Callable

import websocket

from BaseConfig import BinanceConfig
from BaseHandler import SymbolStreamCsvHandler
from BaseLogger import BinanceSyncLogger


def convert_defaultdict_to_dict(default_dict):
    if isinstance(default_dict, defaultdict):
        default_dict = {k: convert_defaultdict_to_dict(v) for k, v in default_dict.items()}
    return default_dict


class BinanceSyncStrategy:

    def __init__(self, log_file="log.default", config_file=None, proxy=None, ws_trace=False, debug=False):
        self.config = BinanceConfig()
        if config_file is not None:
            self.config = self.config.load_from_yaml(config_file)

        self.logger = BinanceSyncLogger(log_file)

        if proxy is not None:
            self.proxy = proxy.replace("/", "").split(':')
            print(self.proxy)
            assert len(self.proxy) == 3, "Invalid proxy format, use format like 'http://127.0.0.1:8888'"
        else:
            self.proxy = None

        self.symbols = set()
        self.handlers = dict()
        self.callbacks: dict[str, dict[str, Callable]] = defaultdict(lambda: defaultdict(lambda: self.on_missing))
        self.log_ctrl: dict[str, dict[str, bool]] = defaultdict(lambda: defaultdict(lambda: False))

        self.subscribe_url = "wss://fstream.binance.com/stream?streams="
        websocket.enableTrace(ws_trace)
        self.debug = debug

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


    def init_handler(self, *args, **kwargs):
        raise NotImplementedError("Please register some handlers first.")

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
        )

        print(f"Strategy Start with subscription url: {self.subscribe_url}")
        print(f"CallBacks: \n{pformat(convert_defaultdict_to_dict(self.callbacks))}")

        if self.proxy is None:
            ws.run_forever()
        else:
            print(f"Using proxy: {self.proxy}")
            ws.run_forever(
                http_proxy_host=self.proxy[1],
                http_proxy_port=self.proxy[2],
                proxy_type=self.proxy[0]
            )

    def _on_message(self, ws, message):

        rec_time = datetime.now()
        message = json.loads(message)
        stream_list = message['stream'].split('@')
        symbol = stream_list[0]
        event = stream_list[1]
        data: dict = message['data']

        if self.log_ctrl[symbol][event]:
            self.logger.log(INFO, message)

        self.callbacks[symbol][event](symbol, event, data, rec_time)

    @staticmethod
    def on_missing(symbol: str, name: str, data: dict, rec_time: datetime):
        raise LookupError(f"Missing Callback on {symbol} {name}")

    def on_agg_trade(self, symbol: str, name: str, data: dict, rec_time: datetime):
        raise NotImplementedError

    def on_depth20(self, symbol: str, name: str, data: dict, rec_time: datetime):
        raise NotImplementedError

    def on_force_order(self, symbol: str, name: str, data: dict, rec_time: datetime):
        raise NotImplementedError

    def on_kline(self, symbol: str, name: str, data: dict, rec_time: datetime):
        raise NotImplementedError

    def on_book_ticker(self, symbol: str, name: str, data: dict, rec_time: datetime):
        raise NotImplementedError


class Rec2CsvStrategy(BinanceSyncStrategy):
    handlers: dict[str, SymbolStreamCsvHandler]

    def __init__(self, log_file="log.default", config_file=None, proxy=None, ws_trace=False, debug=False):
        super().__init__(log_file, config_file, proxy, ws_trace, debug)

    def init_handler(self, parent_path):
        if not os.path.exists(parent_path):
            os.makedirs(parent_path)

        self.handlers: dict[str, SymbolStreamCsvHandler] = {
            symbol: SymbolStreamCsvHandler(parent_path, symbol)
            for symbol in ["ethusdt", "btcusdt"]
        }

    def on_agg_trade(self, symbol: str, name: str, data: dict, rec_time: datetime):
        self.handlers[symbol].on_agg_trade.process_line(data)

    def on_depth20(self, symbol: str, name: str, data: dict, rec_time: datetime):
        # print(data)
        self.handlers[symbol].on_depth20.process_line(data)

    def on_force_order(self, symbol: str, name: str, data: dict, rec_time: datetime):
        self.handlers[symbol].on_force_order.process_line(data)

    def on_kline(self, symbol: str, name: str, data: dict, rec_time: datetime):
        pass
        # self.handlers[symbol].on_kline.process_line(data)

    def on_book_ticker(self, symbol: str, name: str, data: dict, rec_time: datetime):
        self.handlers[symbol].on_book_ticker.process_line(data)


if __name__ == '__main__':
    s = Rec2CsvStrategy(proxy="http://i.**REMOVED**:7890", log_file="log.txt", ws_trace=False)
    s.subscribe("ethusdt", "depth20@100ms", write_to_log=True)
    s.subscribe("ethusdt", "aggTrade", write_to_log=True)
    s.init_handler("./tmp_folder")
    s.run()
