import hashlib
import hmac

import backoff
import httpx
from httpx import Response

from binance_td.future.utils import config
from tools import *


class BinanceFutureTradingAPIUtils:
    symbol_all = []
    symbol_info = dict()

    def __init__(self):

        self.base_url = 'https://fapi.binance.com'
        self.client = httpx.Client(proxies=global_config.proxies)
        self.api_key = config.api_key
        self.api_secret = config.api_secret
        self.headers = {
            "X-MBX-APIKEY": self.api_key
        }
        self.logger = get_logger("future_td")

        # self.api_cache = Cache(config.api_cache_dir)
        self.order_cache = Cache(f"{global_config.future_local_order_cache}")
        self.last_order_id = max(self.order_cache.iterkeys(), default=1_000_000)
        self.init_exchange_info()

    def __del__(self):
        pass
        # self.client.close()

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

    @backoff.on_exception(backoff.expo, httpx.ConnectTimeout, max_tries=5)
    def get(self, url, params=None, auth=False) -> dict:

        if rate_limit.is_limited():
            time.sleep(10)

        if params is None:
            params = dict()

        url = self.base_url + url
        timestamp = int(time.time() * 1000)

        if auth:
            params['timestamp'] = timestamp
            params['signature'] = self.generate_signature(params)
            response: Response = self.client.get(url, params=params, headers=self.headers)
        else:
            response: Response = self.client.get(url, params=params)

        resp = response.json()
        rate_limit.update(response.headers)

        if response.status_code == 200:
            text = f"GET request successful. url: {url}, param: {params}, resp: {resp}."[:512]
            self.logger.info(text)
            return resp
        else:
            self.logger.error(
                f"GET Request failed with status code: {response.status_code}. url: {url}, param: {params}.")

    def post(self, url, data=None, recv_window=None, auth=False):

        if rate_limit.is_limited():
            global_logger.critical(f"API access rate exceeds limit when sending order.")
            time.sleep(5)

        url = self.base_url + url
        timestamp = int(time.time() * 1000)

        if data is None:
            data = dict()

        if recv_window is None:
            data['recvWindow'] = 3000

        if auth:
            data['timestamp'] = timestamp
            data['signature'] = self.generate_signature(data)
            response: Response = self.client.post(url, data=data, headers=self.headers)
        else:
            response: Response = self.client.post(url, data=data)

        resp = response.json()
        rate_limit.update(response.headers)

        if response.status_code == 200:
            self.logger.info(
                f"POST request successful. url: {url}, data: {data}, resp: {resp}, resp_header: {response.headers}."
            )
        else:
            self.logger.error(
                f"POST Request failed with status code: {response.status_code}, "
                f"binance code: {resp['code']} - {resp['msg']} with "
                f" url: {url}, data: {data}, resp: {resp}."
            )

        return resp

    def put(self, url, data=None, recv_window=None, auth=False):

        if rate_limit.is_limited():
            global_logger.critical(f"API access rate exceeds limit when sending order.")
            time.sleep(5)

        url = self.base_url + url
        timestamp = int(time.time() * 1000)

        if data is None:
            data = dict()

        if recv_window is None:
            data['recvWindow'] = 3000

        if auth:
            data['timestamp'] = timestamp
            data['signature'] = self.generate_signature(data)
            response: Response = self.client.put(url, data=data, headers=self.headers)
        else:
            response: Response = self.client.put(url, data=data)

        resp = response.json()
        rate_limit.update(response.headers)

        if response.status_code == 200:
            self.logger.info(
                f"PUT request successful. url: {url}, data: {data}, resp: {resp}, resp_header: {response.headers}."
            )
        else:
            self.logger.error(
                f"PUT Request failed with status code: {response.status_code}, "
                f"binance code: {resp['code']} - {resp['msg']} with "
                f" url: {url}, data: {data}, resp: {resp}."
            )

        return resp

    def delete(self, url, data=None, recv_window=None, auth=False):

        if rate_limit.is_limited():
            global_logger.critical(f"API access rate exceeds limit when deleting order.")
            time.sleep(5)

        url = self.base_url + url
        timestamp = int(time.time() * 1000)

        if data is None:
            data = dict()

        if recv_window is None:
            data['recvWindow'] = 5000

        if auth:
            data['timestamp'] = timestamp
            data['signature'] = self.generate_signature(data)
            response: Response = self.client.delete(url, params=data, headers=self.headers)
        else:
            response: Response = self.client.delete(url, params=data)

        resp = response.json()
        rate_limit.update(response.headers)

        if response.status_code == 200:
            self.logger.info(
                f"DELETE request successful. url: {url}, data: {data}, resp: {resp}, resp_header: {response.headers}."
            )
        else:
            self.logger.error(
                f"DELETE Request failed with status code: {response.status_code}, "
                f"binance code: {resp['code']} - {resp['msg']} with "
                f" url: {url}, data: {data}, resp: {resp}."
            )

        return resp

    def check_delay(self, retry_times=3):
        delay_ls = [- (self.get_server_time()["serverTime"] - get_ms()) for _ in range(retry_times)]
        return sum(delay_ls) / len(delay_ls), max(delay_ls)

    def get_server_time(self):
        """
        API 获取服务器时间
        :return: "serverTime": 1499827319559
        """
        return self.get("/fapi/v1/time")

    def get_exchange_info(self):
        """
        API 获取交易规则和交易对
        """
        return self.get("/fapi/v1/exchangeInfo")

    def init_exchange_info(self):
        """
        初始化交易对信息
        """
        exchange_info = self.get_exchange_info()

        # pprint(exchange_info)

        for symbol_dic in exchange_info['symbols']:
            if symbol_dic['contractType'] == 'PERPETUAL':
                symbol = symbol_dic['symbol'].lower()
                self.symbol_info[symbol] = symbol_dic
                self.symbol_all.append(symbol)

        delay_avg, delay_max = self.check_delay()

        self.logger.info(
            f"Init exchange info success, "
            f"total {len(self.symbol_all)} symbols, from {self.symbol_all[0]} to {self.symbol_all[-1]}, "
            f"avg delay {delay_avg:.2f} ms, max delay {delay_max:.2f} ms"
        )

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
        # TODO: save balance info
        return data

    def get_account(self):
        data = self.get("/fapi/v2/account", auth=True)
        # TODO: save account info
        return data

    def set_leverage(self, symbol, leverage):
        params = dict(
            symbol=symbol,
            leverage=leverage
        )
        self.post("/fapi/v1/leverage", data=params, auth=True)

    def get_income_last_7day(self):
        """
        weight: 30
        """
        params = dict(
            limit=1000
        )
        data = self.post("/fapi/v1/income", data=params, auth=True)
        # TODO: update income info
        return data

    def get_trading_limit_status(self):
        data = self.post("/fapi/v1/apiTradingStatus", data=dict(), auth=True)
        # TODO: update trading limit status
        return data

    @staticmethod
    def _step_filter(value, min_value, step_value):
        """
        修正发单量价数据以符合步进 (step) 要求

        """
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
                    self.logger.warning(f"{symbol} price {price} is not valid in [{p1}, {p2}]")
                price = self._step_filter(price, p1, sp)
                price = round(price, self.symbol_info[symbol]['pricePrecision'])

            elif filter_type == 'LOT_SIZE':
                q1 = float(_filter['minQty'])
                q2 = float(_filter['maxQty'])
                sq = float(_filter['stepSize'])

                if not q1 <= quantity <= q2:
                    status = 5002
                    self.logger.warning(f"{symbol} quantity {quantity} is not valid in [{q1}, {q2}]")
                quantity = self._step_filter(quantity, q1, sq)
                quantity = round(quantity, self.symbol_info[symbol]['quantityPrecision'])

            elif filter_type == 'MIN_NOTIONAL':
                if price * quantity < float(_filter['notional']):
                    status = 5003
                    self.logger.warning(
                        f"{symbol} price {price} quantity {quantity} notional is not valid, "
                        f"total amount is {price * quantity} which smaller than {_filter['notional']}"
                    )

            else:
                pass

            # NotImplemented
            # 'MARKET_LOT_SIZE'
            # 'MAX_NUM_ORDERS'
            # 'MAX_NUM_ALGO_ORDERS'
            # 'PERCENT_PRICE'

        return symbol, price, quantity, status

    def generate_limit_order_params_v1(
            self, symbol, price, quantity, order_side, client_order_id, time_in_force, gtd_second, local_order_id
    ):

        if local_order_id == 'auto':
            local_order_id = self.last_order_id = self.last_order_id + 1

        symbol, price, quantity, filter_status = self.order_filter(symbol, price, quantity)
        params = dict(
            symbol=symbol,
            side=order_side,
            type=OrderType.LIMIT,
            price=price,
            quantity=quantity,
            newClientOrderId=client_order_id,
            timeInForce=time_in_force
        )
        if time_in_force == TimeInForce.GTD:
            if gtd_second is None or gtd_second <= 600:
                self.logger.error("gtd_second must be set and > 600 when use TimeInForce.GTD. using default 601 second")
                gtd_second = 601
            params['goodTillDate'] = int((time.time() + gtd_second) * 1000)

        return params, filter_status, local_order_id

    def send_limit_order_v1(
            self,
            symbol,
            price,
            quantity,
            order_side,
            client_order_id,
            time_in_force=TimeInForce.GTC,
            gtd_second=None,
            local_order_id='auto',
            testnet=False,
    ):

        url = "/fapi/v1/order/test" if testnet else "/fapi/v1/order"

        params, filter_status, local_order_id = self.generate_limit_order_params_v1(
            symbol, price, quantity, order_side, client_order_id, time_in_force, gtd_second, local_order_id
        )
        self.logger.info(f"generate order params: {params}")

        if filter_status == 2000:
            resp = self.post(url, params, auth=True)
        else:
            resp = dict(
                code=filter_status,
                status=OrderStatus.NOT_SEND
            )
            self.logger.error(f"Invalid price {price} and quantity {quantity}, error code {filter_status}")

        if 'updateTime' in resp:
            resp['code'] = 0
        else:
            resp['updateTime'] = get_ms()
            resp['status'] = OrderStatus.REJECTED

        self.order_cache[local_order_id] = dict(
            order_status=resp['status'],
            update_time=resp['updateTime'],
            resp_code=resp['code'],
            params=params,
            response=resp
        )

        return resp

    def send_market_order_v1(
            self,
            symbol,
            quantity,
            order_side,
            client_order_id,
            local_order_id='auto',
            testnet=False,
    ):
        url = "/fapi/v1/order/test" if testnet else "/fapi/v1/order"

        if local_order_id == 'auto':
            local_order_id = self.last_order_id = self.last_order_id + 1

        params = dict(
            symbol=symbol,
            side=order_side,
            type=OrderType.MARKET,
            quantity=quantity,
            newClientOrderId=client_order_id,
        )

        resp = self.post(url, params, auth=True)

        if 'updateTime' in resp:
            resp['code'] = 0
        else:
            resp['updateTime'] = get_ms()
            resp['status'] = OrderStatus.REJECTED

        self.order_cache[local_order_id] = dict(
            order_status=resp['status'],
            update_time=resp['updateTime'],
            resp_code=resp['code'],
            params=params,
            response=resp
        )

        return resp

    def send_batch_limit_order_v1(self):
        raise NotImplementedError

    def put_order_v1(
            self,
            symbol,
            price,
            quantity,
            order_side,
            client_order_id,
            local_order_id='auto'
    ):
        url = "/fapi/v1/order"

        if local_order_id == 'auto':
            local_order_id = self.last_order_id = self.last_order_id + 1

        params = dict(
            symbol=symbol,
            side=order_side,
            price=price,
            quantity=quantity,
            origClientOrderId=client_order_id,
        )

        resp = self.put(url, params, auth=True)

        if 'updateTime' in resp:
            resp['code'] = 0
        else:
            resp['updateTime'] = get_ms()
            resp['status'] = OrderStatus.REJECTED

        self.order_cache[local_order_id] = dict(
            order_status=resp['status'],
            update_time=resp['updateTime'],
            resp_code=resp['code'],
            params=params,
            response=resp
        )

        return resp

    def delete_order_v1(self, symbol, client_order_id):

        url = "/fapi/v1/order"

        params = dict(
            symbol=symbol,
            origClientOrderId=client_order_id
        )

        resp = self.delete(url, params, auth=True)

        if 'updateTime' in resp:
            resp['code'] = 0
        else:
            resp['updateTime'] = int(time.time() * 1000)
            resp['status'] = OrderStatus.REJECTED

        order_id = self.last_order_id = self.last_order_id + 1

        self.order_cache[order_id] = dict(
            order_status=resp['status'],
            update_time=resp['updateTime'],
            err_code=resp['code'],
            params=params,
            response=resp
        )

        return resp

    def delete_all_orders(self, symbol):

        url = "/fapi/v1/allOpenOrders"

        params = dict(
            symbol=symbol,
        )

        resp = self.delete(url, params, auth=True)

        if 'updateTime' in resp:
            resp['code'] = 0
        else:
            resp['updateTime'] = int(time.time() * 1000)
            resp['status'] = OrderStatus.REJECTED

        order_id = self.last_order_id = self.last_order_id + 1

        self.order_cache[order_id] = dict(
            order_status=resp['status'],
            update_time=resp['updateTime'],
            err_code=resp['code'],
            params=params,
            response=resp
        )

        return order_id

    def countdown_cancel_all(self):
        raise NotImplementedError


if __name__ == '__main__':
    import time

    FT = BinanceFutureTradingAPIUtils()
    FT.get_server_time()
    print(FT.symbol_all)
    # print(FT.symbol_info['ETHUSDT'])
    # s.get_all_history_order()
    # s.get_balance()
    # print(s.symbol_info['ETHUSDT'])
    # print(s.symbol_all)
    FT.send_limit_order_v1(
        symbol='ethusdt',
        price=1500.00,
        quantity=6,
        order_side=OrderSide.BUY,
        client_order_id='test1',
        time_in_force=TimeInForce.GTC
    )
    # time.sleep(10)
    # s.delete_order_v1(
    #     delete_last_order=True
    # )
