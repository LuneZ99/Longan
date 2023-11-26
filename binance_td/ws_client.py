import hashlib
import hmac
import json
import random
import signal
import threading
import time

import httpx
import websocket
from httpx import Response
from diskcache import Cache

from binance_td.utils import config, logger
from litchi_md.client import LitchiClientSender
from tools import global_config, get_ms

# websocket.enableTrace(True)


class ListenKeyREST:

    def __init__(self):

        self.url = 'https://fapi.binance.com/fapi/v1/listenKey'

        self.client = httpx.Client(proxies=global_config.proxies)
        self.api_key = config.api_key
        self.api_secret = config.api_secret
        self.listen_key = None

        self.headers = {
            "X-MBX-APIKEY": self.api_key
        }

    def __del__(self):
        self.client.close()

    def generate_signature(self, params):
        query_string = '&'.join(f'{key}={value}' for key, value in params.items())
        message = query_string.encode('utf-8')  # query string
        key_bytes = self.api_secret.encode()
        return hmac.new(key_bytes, message, hashlib.sha256).hexdigest()

    def post_listen_key(self):
        data = dict(timestamp=int(time.time() * 1000))
        data['signature'] = self.generate_signature(data)
        response: Response = self.client.post(self.url, data=data, headers=self.headers)
        self.listen_key = response.json()['listenKey']
        return self.listen_key

    def delete_listen_key(self):
        try:
            data = dict(timestamp=int(time.time() * 1000))
            data['signature'] = self.generate_signature(data)
            self.client.delete(self.url, params=data, headers=self.headers)
        except Exception as e:
            pass

    def put_listen_key(self):
        data = dict(timestamp=int(time.time() * 1000))
        data['signature'] = self.generate_signature(data)
        r = self.client.put(self.url, params=data, headers=self.headers)
        return r.status_code


class BinanceTDWSClient:
    ws: websocket.WebSocketApp
    running = False
    scheduled_task_running = False

    def __init__(self, proxy):

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

        self.listen_key_server = ListenKeyREST()
        self.listen_key = self.listen_key_server.post_listen_key()
        self.subscribe_url = f"wss://fstream.binance.com/ws/{self.listen_key}"
        logger.info(f"Subscribe to {self.subscribe_url}")

        self.litchi_client = LitchiClientSender("future_td_ws", logger=logger)
        self.balance = Cache(f'{global_config.disk_cache_dir}/balance')
        self.position = Cache(f'{global_config.disk_cache_dir}/position')
        self.account_update = Cache(f'{global_config.disk_cache_dir}/account_update')
        self.order_trade_update = Cache(f'{global_config.disk_cache_dir}/account_update')

    def __del__(self):
        self.listen_key_server.delete_listen_key()
        self.litchi_client.close()

    def update_listen_key(self):
        while self.scheduled_task_running:
            try:
                resp_code = self.listen_key_server.put_listen_key()
                if resp_code == 200:
                    logger.info("Update listen key successfully.")
                    time.sleep(60 * 10)
                else:
                    logger.warning("Update listen key failed.")
                    time.sleep(60)
            except httpx.ConnectTimeout as e:
                logger.warning(f"Update listen key failed. {e}")
                time.sleep(60)

    def _fetch_account_update(self):
        self.last_account_update_id = get_ms()
        data = {
            "method": "REQUEST",
            "params":
                [
                    f"{self.listen_key}@account",
                    f"{self.listen_key}@balance",
                    f"{self.listen_key}@position"
                ],
            "id": self.last_account_update_id
        }
        self.ws.send(json.dumps(data))

    def fetch_account_update(self):
        while self.scheduled_task_running:
            self._fetch_account_update()
            time.sleep(10)

    def run(self):

        self.ws = websocket.WebSocketApp(
            self.subscribe_url,
            on_message=self._on_message,
            on_open=self._on_open,
            on_close=self._on_close,
        )
        self.running = True
        proxy = self.proxy[0]
        logger.info(f"Using proxy: {proxy}")

        while self.running:
            self.ws.run_forever(
                http_proxy_host=proxy[1],
                http_proxy_port=proxy[2],
                proxy_type=proxy[0],
                skip_utf8_validation=True
            )

    def _on_open(self, ws):
        self.scheduled_task_running = True
        threads = [
            threading.Thread(target=self.update_listen_key),
            threading.Thread(target=self.fetch_account_update)
        ]
        for thread in threads:
            thread.start()
        # self._fetch_account_update()

    def _on_message(self, ws, message):

        if not self.running:
            self.scheduled_task_running = False
            self.ws.close()
            return

        logger.info(message[:128])
        message = json.loads(message)
        rec_time = get_ms()

        # OT_UPDATE
        if 'e' in message.keys():
            if message['e'] == "ORDER_TRADE_UPDATE":
                event = message['e']
                symbol = message['o']['s']
                message['o']['E'] = message['E']
                processed_msg = {
                    "symbol": symbol,
                    "event": event,
                    "rec_time": rec_time,
                    "data": message['o'],
                }
                self.litchi_client.broadcast(processed_msg)
                self.order_trade_update[message['o']['c']] = message
            elif message['e'] == "ACCOUNT_UPDATE":
                self.account_update[f"{message['a']['m']}_{message['E']}"] = message
                # todo save all account_update
        elif 'id' in message.keys() and message['id'] == self.last_account_update_id:
            for res in message['result']:
                if 'balance' in res['req']:
                    for val in res['res']['balances']:
                        self.balance[val['asset']] = val
                elif 'position' in res['req']:
                    for val in res['res']['positions']:
                        self.position[val['symbol']] = val

            pass

    def _on_close(self, ws, code, message):
        pass


if __name__ == '__main__':
    tws = BinanceTDWSClient(global_config.proxy_url)

    def keyboard_interrupt_handler(signal, frame):
        print("Keyboard interrupt received. Stopping...")
        global tws
        tws.running = False

    signal.signal(signal.SIGINT, keyboard_interrupt_handler)

    tws.run()
