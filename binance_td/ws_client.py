import hashlib
import hmac
import itertools
import time
from pprint import pprint

import httpx
import websocket
from diskcache import Cache
from httpx import Response

from tools import *
import logging
from binance_td.utils import config


logger = logging.getLogger('logger_td_ws')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s | %(message)s')

file_handler = logging.FileHandler("td_ws.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


class ListenKeyREST:

    def __init__(self):

        self.url = 'https://fapi.binance.com/fapi/v1/listenKey'

        self.client = httpx.Client(proxies=config.proxies)
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
            r = self.client.delete(self.url, params=data, headers=self.headers)
            print(r)
        except Exception as e:
            pass


    def put_listen_key(self):
        data = dict(timestamp=int(time.time() * 1000))
        data['signature'] = self.generate_signature(data)
        r = self.client.put(self.url, params=data, headers=self.headers)
        print(r)


class BinanceTDWSClient:

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

    def __del__(self):
        self.listen_key_server.delete_listen_key()
        logger.error(f"Websocket disconnected, retrying ...")

    def update_listen_key(self):
        self.listen_key_server.put_listen_key()

    def run(self):

        ws = websocket.WebSocketApp(
            self.subscribe_url,
            on_message=self._on_message,
            on_open=self._on_open,
            on_close=self._on_close,
        )

        # for proxy in itertools.cycle(self.proxy):
        proxy = self.proxy[0]
        logger.info(f"Using proxy: {proxy}")
        ws.run_forever(
            http_proxy_host=proxy[1],
            http_proxy_port=proxy[2],
            proxy_type=proxy[0],
            skip_utf8_validation=True
        )
        ws.close()
        logger.error(f"Websocket disconnected, retrying ...")

    def _on_open(self, ws):
        pass

    def _on_message(self, ws, message):
        print(message)
        pass

    def _on_close(self, ws, code, message):
        print(message)
        pass


if __name__ == '__main__':
    tws = BinanceTDWSClient(config.proxy_url)
    tws.run()



