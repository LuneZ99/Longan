import hashlib
import hmac
import time
from pprint import pprint

import httpx
from diskcache import Cache
from tools import *

from binance_td.utils import config, logger_td


class BinanceAPIUtils:
    symbol_all = []

    def __init__(self, flush_api_cache=False, flush_utils_cache=False):
        self.base_url = 'https://fapi.binance.com'

        proxies = {
            "http://": "http://127.0.0.1:7890",
            "https://": "http://127.0.0.1:7890"
        }

        self.client = httpx.Client(proxies=proxies)

        # example use
        # self.api_key = "Gy4D9ha8BGOwu4dMNhBMquZPimmis9m3qQChZiUIhSY6Zdt4dpKV41KvMEgVjP7i"
        # self.api_secret = "Yo5xehT997NqS8yzSvelLS1Jl59bSrJbzLuY4YX4OkQ2UbmgNj2QLXVPHk1oAvfW"

        self.api_key = config.api_key
        self.api_secret = config.api_secret

        self.headers = {
            "X-MBX-APIKEY": self.api_key
        }

        self.api_cache = Cache()
        self.cache = Cache()

        if 'order_all' in self.cache:
            self.last_order_id = max(self.cache["order_all"].keys())
        else:
            self.cache['order_all'] = dict()
            self.last_order_id = 1_000_000

        print(self.cache['order_all'])

        self.symbol_info = dict()
        self.init_exchange_info()

    def generate_signature(self, params):

        # 将参数按照字典顺序排序并拼接成字符串
        # sorted_params = sorted(params.items())
        # query_string = '&'.join(f'{key}={value}' for key, value in sorted_params)

        # 实际上无需按顺序
        sorted_params = params.items()
        query_string = '&'.join(f'{key}={value}' for key, value in sorted_params)

        # 计算 HMAC SHA256 签名
        message = query_string.encode('utf-8')  # query string
        key_bytes = self.api_secret.encode()
        signature = hmac.new(key_bytes, message, hashlib.sha256).hexdigest()

        return signature

    def get(self, url, params=None, auth=False, use_cache=False) -> dict:
        url = self.base_url + url
        cache_key = f"{url}_{params}"

        if use_cache:
            data = self.api_cache.get(cache_key, None)
            if data is not None:
                print(f"Use cache key: {cache_key}")
                return data

        if params is None:
            params = dict()

        if auth:
            params['timestamp'] = int(time.time() * 1000)
            params['signature'] = self.generate_signature(params)
            response = self.client.get(url, params=params, headers=self.headers)
        else:
            response = self.client.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            self.api_cache[cache_key] = data
            return data
        else:
            print(f"Request failed with status code: {response.status_code}")

    def post(self, url, data=None, recv_window=None, auth=False):

        url = self.base_url + url

        if data is None:
            data = dict()

        if recv_window is None:
            data['recvWindow'] = 5000

        if auth:
            data['timestamp'] = int(time.time() * 1000)
            data['signature'] = self.generate_signature(data)
            response = self.client.post(url, data=data, headers=self.headers)
        else:
            response = self.client.post(url, data=data)

        if response.status_code == 200:
            resp = response.json()
            return resp
        else:
            resp = response.json()
            print(
                f"Request failed with status code: {response.status_code}, "
                f"binance code: {resp['code']} = {resp['msg']} with "
                f"POST body {data}"
            )
            return resp
            # assert httpx.HTTPError, f"Request {url} - {response.status_code}"

    def get_server_time(self):
        """
        API 获取服务器时间
        :return: "serverTime": 1499827319559
        """
        return self.get("/fapi/v1/time")

    def get_exchange_info(self, use_cache=True):
        """
        API 获取交易规则和交易对
        """
        return self.get("/fapi/v1/exchangeInfo", use_cache=use_cache)

    def init_exchange_info(self):
        """
        初始化交易对信息
        """
        exchange_info = self.get_exchange_info(use_cache=False)

        for symbol_dic in exchange_info['symbols']:
            if symbol_dic['contractType'] == 'PERPETUAL' and symbol_dic['quoteAsset'] != 'BUSD':
                self.symbol_info[symbol_dic['symbol']] = symbol_dic
                self.symbol_all.append(symbol_dic['symbol'])

    def get_depth(self):
        # API 深度信息
        raise NotImplementedError

    def get_trades(self):
        # API 近期成交
        raise NotImplementedError

    def get_historical_trades(self):
        # API 历史成交(MARKET_DATA)
        raise NotImplementedError

    def get_agg_trades(self):
        # API 近期成交(归集)
        raise NotImplementedError

    def get_k_lines(self, symbol, interval, start_time=None, end_time=None, limit=None):
        """
        API K线数据

        :param symbol: 交易对
        :param interval: 时间间隔 [1m 1h 8h ...]
        :param start_time:
        :param end_time:
        :param limit:
        :return:
        """

        assert all(x is None for x in [start_time, end_time, limit]), "At least one param is not None."
        if limit >= 1000:
            pass

        return self.get(
            "/fapi/v1/klines",
            params=dict(
                symbol=symbol,
                interval=interval,
                startTime=start_time,
                endTime=end_time,
                limit=limit
            ),
            use_cache=True
        )

    def get_continuous_k_line(self):
        # API 连续合约K线数据
        raise NotImplementedError

    def get_index_price_k_line(self):
        # API 价格指数K线数据
        raise NotImplementedError

    def get_mark_price_k_line(self):
        # API 标记价格K线数据
        raise NotImplementedError

    def get_premium_index_k_line(self):
        # API 溢价指数K线数据
        raise NotImplementedError

    def get_premium_index(self, symbol):
        """
        API 最新标记价格和资金费率

        :param symbol:
        :return:
        """

        return self.get(
            "/fapi/v1/premiumIndex",
            params=dict(
                symbol=symbol,
            )
        )

    def get_funding_rate(self):
        # API 资金费率历史
        raise NotImplementedError

    def get_24hr(self):
        # API 24hr价格变动情况
        raise NotImplementedError

    def get_ticker_price(self):
        # API 最新价格
        raise NotImplementedError

    def get_book_ticker(self):
        # API 当前最优挂单
        raise NotImplementedError

    def get_all_history_order(self, symbol=None):
        """

        :param symbol:
        :return:
        """
        return self.get(
            "/fapi/v1/allOrders",
            params=dict(
                symbol=symbol
            ),
            auth=True
        )

    def get_balance(self):
        data = self.get("/fapi/v2/balance", auth=True)
        pprint(data)

    @staticmethod
    def pq_filter(value, min_value, step_value):
        diff = value - min_value
        adjusted_diff = round(diff / step_value) * step_value
        new_value = min_value + adjusted_diff
        return new_value

    def order_filter(self, symbol, price, quantity):
        """
        发单数据格式修正

        :param symbol:
        :param price:
        :param quantity:
        :return: symbol , price, quantity, status
        """
        filters = self.symbol_info[symbol]['filters']
        status = 2000

        for _filter in filters:
            filter_type = _filter['filterType']

            if filter_type == 'PRICE_FILTER':
                p1 = float(_filter['minPrice'])
                p2 = float(_filter['maxPrice'])
                sp = float(_filter['tickSize'])

                if not p1 <= price <= p2:
                    status = 5001
                    print(f"{symbol} price {price} is not valid in [{p1}, {p2}]")
                price = self.pq_filter(price, p1, sp)
                price = round(price, self.symbol_info[symbol]['pricePrecision'])

            elif filter_type == 'LOT_SIZE':
                q1 = float(_filter['minQty'])
                q2 = float(_filter['maxQty'])
                sq = float(_filter['stepSize'])

                if not q1 <= quantity <= q2:
                    status = 5002
                    print(f"{symbol} quantity {quantity} is not valid in [{q1}, {q2}]")
                quantity = self.pq_filter(quantity, q1, sq)
                quantity = round(quantity, self.symbol_info[symbol]['quantityPrecision'])

            elif filter_type == 'MIN_NOTIONAL':
                if price * quantity < float(_filter['notional']):
                    status = 5003
                    print(
                        f"{symbol} price {price} quantity {quantity} notional is not valid, "
                        f"must be larger than {_filter['notional']}"
                    )

            else:
                pass
                # print(_filter)

            # NotImplemented
            # 'MARKET_LOT_SIZE'
            # 'MAX_NUM_ORDERS'
            # 'MAX_NUM_ALGO_ORDERS'
            # 'PERCENT_PRICE'

        return symbol, price, quantity, status

    def generate_limit_order_params_v1(
            self, symbol, price, quantity, order_side, oid, time_in_force, gtd_second, prefix
    ):
        oid = self.last_order_id = self.last_order_id + 1 if oid == 'auto' else oid
        cid = prefix + '_' + str(oid)
        symbol, price, quantity, pq_status = self.order_filter(symbol, price, quantity)
        params = dict(
            symbol=symbol,
            side=order_side.value,
            type=OrderType.LIMIT.value,
            price=price,
            quantity=quantity,
            newClientOrderId=cid,
            timeInForce=time_in_force.value
        )
        if time_in_force == TimeInForce.GTD:
            assert gtd_second is not None, "gtd_second must be set when use TimeInForce.GTD"
            params['goodTillDate'] = int((time.time() + gtd_second) * 1000)
        return params, pq_status, oid

    def send_limit_order_v1(
            self,
            symbol,
            price,
            quantity,
            order_side: OrderSide,
            oid='auto',
            time_in_force=TimeInForce.GTC,
            gtd_second=None,
            prefix='',
            testnet=False,

    ):
        url = "/fapi/v1/order/test" if testnet else "/fapi/v1/order"

        params, pq_status, oid = self.generate_limit_order_params_v1(
            symbol, price, quantity, order_side, oid, time_in_force, gtd_second, prefix
        )
        print(params)

        if pq_status == 2000:
            resp = self.post(url, params, auth=True)
        else:
            resp = dict(
                code=pq_status,
                status=OrderStatus.NOT_SEND.value
            )
            print(f"Invalid price {price} and quantity {quantity}, error code {pq_status}")

        if 'updateTime' in resp:
            resp['code'] = -1
        else:
            resp['updateTime'] = int(time.time() * 1000)

        pprint(resp)
        print(oid)

        self.cache["order_all"][oid] = dict(
            order_status=resp['status'],
            update_time=resp['updateTime'],
            err_code=resp['code'],
            params=params,
            response=resp
        )

        print(self.cache["order_all"][oid])

    def send_batch_limit_order_v1(self):
        raise NotImplementedError


if __name__ == '__main__':
    s = BinanceAPIUtils()
    s.get_server_time()
    # s.get_all_history_order()
    # s.get_balance()
    print(s.symbol_info['ETHUSDT'])
    print(s.symbol_all)
    # s.send_limit_order_v1(
    #     symbol='ETHUSDT',
    #     price=1600.00,
    #     quantity=0.01,
    #     order_side=OrderSide.BUY,
    #     time_in_force=TimeInForce.GTD,
    #     gtd_second=660,
    #     prefix='test2',
    #     # testnet=True
    # )

