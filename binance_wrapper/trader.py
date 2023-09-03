import httpx
import hmac
import hashlib
from datetime import datetime
import time
from pprint import pprint
from diskcache import Cache


class BinanceTrader:

    def __init__(self):
        self.base_url = 'https://fapi.binance.com'

        proxies = {
            "http://": "http://127.0.0.1:7890",
            "https://": "http://127.0.0.1:7890"
        }

        self.client = httpx.Client(proxies=proxies)

        self.api_key = "**REMOVED**"
        self.api_secret = "**REMOVED**"

        self.headers = {
            "X-MBX-APIKEY": self.api_key
        }

        self.order_cache = Cache()

    def generate_signature(self, params):

        # 将参数按照字典顺序排序并拼接成字符串
        sorted_params = sorted(params.items())
        query_string = '&'.join(f'{key}={value}' for key, value in sorted_params)

        print(query_string)

        # 计算 HMAC SHA256 签名
        message = query_string.encode('utf-8')  # query string
        key_bytes = self.api_secret.encode()

        signature = hmac.new(key_bytes, message, hashlib.sha256).hexdigest()

        print(signature)

        return signature

    def get(self, url, params=None, auth=False):
        url = self.base_url + url
        print(url)

        if params is None:
            params = dict()

        if auth:
            params['timestamp'] = int(time.time() * 1000)
            signature = self.generate_signature(params)
            params['signature'] = signature
            response = self.client.get(url, params=params, headers=self.headers)
        else:
            response = self.client.get(url, params=params)


        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Request failed with status code: {response.status_code}")

    def post(self, url, params=None, auth=False):
        # todo fix post method


        url = self.base_url + url
        print(url)

        if params is None:
            params = dict()

        if auth:
            params['timestamp'] = int(time.time() * 1000)
            signature = self.generate_signature(params)
            params['signature'] = signature
            response = self.client.post(url, params=params, headers=self.headers)
        else:
            response = self.client.post(url, params=params)


        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Request failed with status code: {response.status_code}")
            assert httpx.HTTPError, f"Request {url} - {response.status_code}"


    def get_server_time(self):
        data = self.get("/fapi/v1/time")
        print(data)

    def get_all_history_order(self):

        params = dict(
            symbol="ETHUSDT"
        )

        data = self.get("/fapi/v1/allOrders", params=params, auth=True)

        print(data)

    def get_balance(self):
        data = self.get("/fapi/v2/balance", auth=True)
        pprint(data)


    def send_order(
            self, symbol, order_side, order_type, price, quantity,
            oid=None, testnet=False
    ):
        url = "/fapi/v1/order/test" if testnet else "/fapi/v1/order"



        pass


    def send_batch_order(self):
        pass





if __name__ == '__main__':
    s = BinanceTrader()
    s.get_server_time()
    s.get_all_history_order()
    s.get_balance()

