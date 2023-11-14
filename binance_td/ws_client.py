import hashlib
import hmac
import json
import threading
import time

import httpx
import websocket
from httpx import Response

from binance_td.utils import config, logger
from litchi_md.client import LitchiClientSender
from tools import global_config, MsgType, RegisterType, get_ms


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

        self.litchi_client = LitchiClientSender("binance_td_ws", logger=logger)
        self.litchi_client.send_str(f"{MsgType.register}{RegisterType.sender}")

        if config.push_to_litchi:
            try:
                self.litchi_md = websocket.create_connection(config.litchi_md_url)
                self.litchi_md.send(f"{MsgType.register}{RegisterType.sender}")
                logger.info("litchi_md connected")
            except ConnectionRefusedError:
                logger.warning("ConnectionRefusedError, is litchi_md server running?")
                self.litchi_md = None
        else:
            self.litchi_md = None

    def __del__(self):
        self.listen_key_server.delete_listen_key()

    def update_listen_key(self):
        while True:
            resp_code = self.listen_key_server.put_listen_key()
            if resp_code == 200:
                logger.info("Update listen key successfully.")
                time.sleep(60 * 10)
            else:
                logger.warning("Update listen key failed.")
                time.sleep(60)

    def run(self):

        self.ws = websocket.WebSocketApp(
            self.subscribe_url,
            on_message=self._on_message,
            on_open=self._on_open,
            on_close=self._on_close,
        )

        proxy = self.proxy[0]
        logger.info(f"Using proxy: {proxy}")
        self.ws.run_forever(
            http_proxy_host=proxy[1],
            http_proxy_port=proxy[2],
            proxy_type=proxy[0],
            skip_utf8_validation=True
        )

    def _on_open(self, ws):
        thread = threading.Thread(target=self.update_listen_key)
        thread.start()

    def _on_message(self, ws, message):

        message = json.loads(message)
        logger.info(message)
        rec_time = get_ms()
        event = message['e']

        # OT_UPDATE
        if event == "ORDER_TRADE_UPDATE":
            symbol = message['o']['s']
            message['o']['E'] = message['E']
        else:
            return
        processed_msg = {
            "symbol": symbol,
            "event": event,
            "rec_time": rec_time,
            "data": message['o'],
        }
        self.litchi_client.broadcast(processed_msg)

    def _on_close(self, ws, code, message):
        pass


if __name__ == '__main__':
    tws = BinanceTDWSClient(config.proxy_url)
    tws.run()
