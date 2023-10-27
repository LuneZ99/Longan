import hashlib
import hmac
import time
from pprint import pprint

import httpx
from diskcache import Cache
from httpx import Response

from tools import *

from binance_td.utils import config
import logging


logger = logging.getLogger('logger_td')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s | %(message)s')

file_handler = logging.FileHandler("td.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


class BinanceTradingAPIUtils:
    symbol_all = []
    symbol_info = dict()

    def __init__(self):

        self.base_url = 'https://fapi.binance.com'

        self.client = httpx.Client(proxies=config.proxies)

        self.api_key = config.api_key
        self.api_secret = config.api_secret

        self.headers = {
            "X-MBX-APIKEY": self.api_key
        }

        self.api_cache = Cache(config.api_cache_dir)
        self.order_cache = Cache(config.order_cache_dir)
        self.last_order_id = max(self.order_cache.iterkeys()) if len(self.order_cache) > 0 else 1_000_000
        self.init_exchange_info()

        self.used_weight_1m = 0
        self.order_count_10s = 0
        self.order_count_1m = 0

    def __del__(self):
        self.client.close()

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
        if params is None:
            params = dict()

        url = self.base_url + url
        timestamp = int(time.time() * 1000)
        cache_key = f"GET_{url}_{params}_{timestamp}"

        if use_cache:
            resp = self.api_cache.get(cache_key, None)
            if resp is not None:
                logger.info(f"Use cache key: {cache_key}")
                return resp

        if auth:
            params['timestamp'] = timestamp
            params['signature'] = self.generate_signature(params)
            response: Response = self.client.get(url, params=params, headers=self.headers)
        else:
            response: Response = self.client.get(url, params=params)

        if response.status_code == 200:
            resp = response.json()
            self.api_cache[cache_key] = resp
            # read rate limit from header
            text = f"GET request successful. url: {url}, param: {params}, resp: {resp}."
            if len(text) > 512:
                text = text[:512]
            logger.info(text)
            return resp
        else:
            self.api_cache[cache_key] = None
            logger.error(f"GET Request failed with status code: {response.status_code}. url: {url}, param: {params}.")

    def post(self, url, data=None, recv_window=None, auth=False):

        url = self.base_url + url
        timestamp = int(time.time() * 1000)
        cache_key = f"POST_{url}_{timestamp}"

        if data is None:
            data = dict()

        if recv_window is None:
            data['recvWindow'] = 5000

        if auth:
            data['timestamp'] = timestamp
            data['signature'] = self.generate_signature(data)
            response: Response = self.client.post(url, data=data, headers=self.headers)
        else:
            response: Response = self.client.post(url, data=data)

        resp = response.json()
        head = response.headers

        if response.status_code == 200:
            logger.info(
                f"POST request successful. url: {url}, data: {data}, resp: {resp}, resp_header: {head}."
            )
        else:
            logger.error(
                f"POST Request failed with status code: {response.status_code}, "
                f"binance code: {resp['code']} - {resp['msg']} with "
                f" url: {url}, data: {data}, resp: {resp}."
            )

        # read rate limit from header
        self.used_weight_1m = int(head['x-mbx-used-weight-1m'])
        self.order_count_10s = int(head['x-mbx-order-count-10s'])
        self.order_count_1m = int(head['x-mbx-order-count-1m'])

        self.api_cache[cache_key] = resp
        return resp

    def delete(self, url, data=None, recv_window=None, auth=False):

        url = self.base_url + url
        timestamp = int(time.time() * 1000)
        cache_key = f"DELETE_{url}_{timestamp}"

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
        head = response.headers

        if response.status_code == 200:
            logger.info(
                f"DELETE request successful. url: {url}, data: {data}, resp: {resp}, resp_header: {head}."
            )
        else:
            logger.error(
                f"DELETE Request failed with status code: {response.status_code}, "
                f"binance code: {resp['code']} - {resp['msg']} with "
                f" url: {url}, data: {data}, resp: {resp}."
            )

        self.api_cache[cache_key] = resp
        return resp

    def get_server_time(self):
        """
        API 获取服务器时间
        :return: "serverTime": 1499827319559
        """
        return self.get("/fapi/v1/time")

    def check_delay(self, retry_times=10):
        delay_ls = [- (self.get_server_time()["serverTime"] - get_ms()) for _ in range(retry_times)]
        return sum(delay_ls) / len(delay_ls), max(delay_ls)

    def get_exchange_info(self, use_cache=False):
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
            if symbol_dic['contractType'] == 'PERPETUAL':
                self.symbol_info[symbol_dic['symbol']] = symbol_dic
                self.symbol_all.append(symbol_dic['symbol'])

        delay_avg, delay_max = self.check_delay()

        logger.info(
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
                    logger.warning(f"{symbol} price {price} is not valid in [{p1}, {p2}]")
                price = self._step_filter(price, p1, sp)
                price = round(price, self.symbol_info[symbol]['pricePrecision'])

            elif filter_type == 'LOT_SIZE':
                q1 = float(_filter['minQty'])
                q2 = float(_filter['maxQty'])
                sq = float(_filter['stepSize'])

                if not q1 <= quantity <= q2:
                    status = 5002
                    logger.warning(f"{symbol} quantity {quantity} is not valid in [{q1}, {q2}]")
                quantity = self._step_filter(quantity, q1, sq)
                quantity = round(quantity, self.symbol_info[symbol]['quantityPrecision'])

            elif filter_type == 'MIN_NOTIONAL':
                if price * quantity < float(_filter['notional']):
                    status = 5003
                    logger.warning(
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

    def check_insert_rate_limit(self):
        if self.order_count_10s > 30:
            logger.warning(f"Insert order too quickly, {self.order_count_10s} / 10s, {self.order_count_1m} / 1m")
            return True
        return False

    def generate_limit_order_params_v1(
            self, symbol, price, quantity, order_side, order_id, time_in_force, gtd_second, prefix
    ):
        order_id = self.last_order_id = self.last_order_id + 1 if order_id == 'auto' else order_id
        client_order_id = prefix + '_' + str(order_id)
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
                logger.error("gtd_second must be set and > 600 when use TimeInForce.GTD. using default 601 second")
                gtd_second = 601
            params['goodTillDate'] = int((time.time() + gtd_second) * 1000)

        return params, filter_status, order_id

    def send_limit_order_v1(
            self,
            symbol,
            price,
            quantity,
            order_side,
            order_id='auto',
            time_in_force=TimeInForce.GTC,
            gtd_second=None,
            prefix='',
            testnet=False,
    ):
        if self.check_insert_rate_limit():
            return -1

        url = "/fapi/v1/order/test" if testnet else "/fapi/v1/order"

        params, filter_status, order_id = self.generate_limit_order_params_v1(
            symbol, price, quantity, order_side, order_id, time_in_force, gtd_second, prefix
        )
        logger.info(f"generate order params: {params}")

        if filter_status == 2000:
            resp = self.post(url, params, auth=True)
        else:
            resp = dict(
                code=filter_status,
                status=OrderStatus.NOT_SEND
            )
            logger.error(f"Invalid price {price} and quantity {quantity}, error code {filter_status}")

        if 'updateTime' in resp:
            resp['code'] = 0
        else:
            resp['updateTime'] = int(time.time() * 1000)
            resp['status'] = OrderStatus.REJECTED

        self.order_cache[order_id] = dict(
            order_status=resp['status'],
            update_time=resp['updateTime'],
            err_code=resp['code'],
            params=params,
            response=resp
        )

        return order_id

    def send_batch_limit_order_v1(self):
        raise NotImplementedError

    def delete_order_v1(self, cache_order_id=None, delete_last_order=False):

        url = "/fapi/v1/order"

        orig_cache_order = self.order_cache.peekitem()[1] if delete_last_order else self.order_cache[cache_order_id]
        if orig_cache_order['order_status'] != 'New':
            logger.error(f"Last order is not a New insert order, cannot be delete.")
            return

        symbol = orig_cache_order['params']['symbol']
        client_order_id = orig_cache_order['params']['newClientOrderId']

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

        return order_id

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

    s = BinanceTradingAPIUtils()
    s.get_server_time()
    # s.get_all_history_order()
    # s.get_balance()
    # print(s.symbol_info['ETHUSDT'])
    # print(s.symbol_all)
    s.send_limit_order_v1(
        symbol='ETHUSDT',
        price=1000.00,
        quantity=0.01,
        order_side=OrderSide.BUY,
        time_in_force=TimeInForce.GTC,
        prefix='test',
        # testnet=True
    )
    time.sleep(10)
    s.delete_order_v1(
        delete_last_order=True
    )


