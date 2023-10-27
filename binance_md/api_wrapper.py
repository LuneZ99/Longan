import logging

import httpx
from diskcache import Cache
from httpx import Response

from binance_md.utils import config
from tools import *

logger = logging.getLogger('logger_md')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s | %(message)s')

file_handler = logging.FileHandler("md.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


class BinanceMarketDataAPIUtils:
    symbol_all = []
    symbol_info = dict()

    def __init__(self):

        self.base_url = 'https://fapi.binance.com'

        self.client = httpx.Client(proxies=config.proxies)
        self.api_cache = Cache(config.api_cache_dir)
        self.init_exchange_info()

        self.used_weight_1m = 0

    def __del__(self):
        self.client.close()

    def get(self, url, params=None, use_cache=False) -> dict:
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


if __name__ == '__main__':
    import time

    s = BinanceMarketDataAPIUtils()
    s.get_server_time()
    # s.get_all_history_order()
    # s.get_balance()
    # print(s.symbol_info['ETHUSDT'])
    # print(s.symbol_all)

    time.sleep(10)
