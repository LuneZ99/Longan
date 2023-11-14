import threading
import time
import json
import websocket
from tools import *


class LitchiBaseStrategy:
    thread_main: threading.Thread
    ws: websocket.WebSocketApp

    def __init__(self, logger=None, scheduled_task_interval=0.1, litchi_url="ws://localhost:8010"):

        self.logger = logger or get_logger(
            "litchi_base_strategy",
            "/dev/shm/longan_cache/logs/log.litchi_base_strategy"
        )
        self.litchi_url = litchi_url

        self.scheduled_task_running = False
        self.scheduled_task_interval = scheduled_task_interval

        self.running = False

    def _on_open(self, ws):
        ws.broadcast(f"{MsgType.register}{RegisterType.receiver}")
        self.scheduled_task_running = True
        thread = threading.Thread(target=self._scheduled_task)
        thread.start()

    def _on_close(self, ws, p1, p2):
        pass

    def _on_message(self, ws, msg):
        msg = json.loads(msg)
        event = msg['event']

        if not self.running:
            self.scheduled_task_running = False
            self.ws.close()
            return

        if event == 'aggTrade':
            self._on_agg_trade(**msg)
        elif 'kline' in event and msg['data'][-1]:
            self._on_kline(**msg)
        elif event == 'depth20':
            self._on_depth20(**msg)
        elif event == 'ORDER_TRADE_UPDATE':
            self._on_order_trade(**msg)
        else:
            self.on_message(**msg)

    def _on_depth20(self, symbol, event, data, rec_time):
        self.on_depth20(symbol, event, data, rec_time)

    def _on_agg_trade(self, symbol, event, data, rec_time):
        self.on_agg_trade(symbol, event, data, rec_time)

    def _on_kline(self, symbol, event, data, rec_time):
        self.on_kline(symbol, event, data, rec_time)

    def _on_order_trade(self, symbol, event, data, rec_time):
        self.on_order_trade(symbol, event, data, rec_time)

    def _scheduled_task(self):
        while self.scheduled_task_running:
            self.scheduled_task()
            time.sleep(self.scheduled_task_interval)

    def run(self):
        self.ws = websocket.WebSocketApp(
            self.litchi_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_close=self._on_close,
        )
        self.running = True
        self.ws.run_forever(skip_utf8_validation=True)

    def stop(self):
        pass

    def scheduled_task(self):
        raise NotImplementedError

    def on_message(self, symbol, event, data, rec_time):
        raise NotImplementedError

    def on_agg_trade(self, symbol, event, data, rec_time):
        raise NotImplementedError

    def on_kline(self, symbol, event, data, rec_time):
        raise NotImplementedError

    def on_book_ticker(self, symbol, event, data, rec_time):
        raise NotImplementedError

    def on_depth20(self, symbol, event, data, rec_time):
        raise NotImplementedError

    def on_order_trade(self, symbol, event, data, rec_time):
        raise NotImplementedError
