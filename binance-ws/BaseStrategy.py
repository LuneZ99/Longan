import json
from typing import Callable, Optional

import websocket

from BaseConfig import BinanceConfig
from BaseLogger import BinanceAsyncLogger

websocket.enableTrace(True)


class BinanceStrategy:

    def __init__(self, log_file="log.default.txt", config_file=None, proxy=None):
        self.config = BinanceConfig()
        if config_file is not None:
            self.config = self.config.load_from_yaml(config_file)

        self.log_file = BinanceAsyncLogger(log_file)

        if proxy is not None:
            self.proxy = proxy.replace("/", "").split(':')
            print(self.proxy)
            assert len(self.proxy) == 3, "Invalid proxy format, use format like 'http://127.0.0.1:8888'"
        else:
            self.proxy = None

        self.symbols = set()
        self.handles: dict[str, dict[str, Callable]] = dict()
        self.logger_handlers: dict[str, dict[str, bool]] = dict()

        self.subscribe_url = "wss://fstream.binance.com/stream?streams="

    def subscribe(self, symbol: str, channel: str, write_to_log: bool = True, callback: callable = None):

        """
        订阅指定的symbol和channel
        将订阅信息添加到subscribe_url中
        将symbol添加到symbols集合中
        将write_to_log标志添加到logger_handlers字典中，用于指示是否将日志写入日志文件
        如果指定了callback，则将其添加到handles字典中，用于处理收到的数据
        """

        self.subscribe_url += f"{symbol}@{channel}/"
        self.symbols.add(symbol)

        if symbol not in self.logger_handlers:
            self.logger_handlers[symbol] = {}

        if symbol not in self.handles:
            self.handles[symbol] = {}

        self.logger_handlers[symbol][channel.split('@')[0] if "@" in channel else channel] = write_to_log
        if callback:
            self.handles[symbol][channel.split('@')[0] if "@" in channel else channel] = callback

        print(f"Subscribe {symbol} to {channel}")

    def run(self):

        if not self.subscribe_url.endswith('/'):
            raise ValueError("Please subscribe a symbol first.")

        ws = websocket.WebSocketApp(
            self.subscribe_url,
            on_message=self._on_message,
        )

        print(f"run")
        if self.proxy is None:
            ws.run_forever()
        else:
            ws.run_forever(
                http_proxy_host=self.proxy[1],
                http_proxy_port=self.proxy[2],
                proxy_type=self.proxy[0]
            )

    def _on_message(self, ws, message):
        message = json.loads(message)
        stream_list = message['stream'].split('@')
        symbol = stream_list[0]
        name = stream_list[1]
        data: dict = message['data']

        if self.logger_handlers[symbol][name]:
            # write log
            pass

        self.handles[symbol][name](data)

    def on_agg_trade(self, data: dict):
        raise NotImplementedError

    def on_depth20(self, data: dict):
        raise NotImplementedError


if __name__ == '__main__':
    s = BinanceStrategy(proxy="http://i.**REMOVED**:7890")
    s.subscribe("ethusdt", "depth20@100ms", write_to_log=True, callback=s.on_depth20)
    s.run()
