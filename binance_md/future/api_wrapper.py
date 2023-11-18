from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime

import httpx
from diskcache import Cache
from httpx import Response, ConnectTimeout

from binance_md.future.utils import config, logger
from tools import global_config, get_ms, get_timestamp_list_hour, split_list_by_length


class BinanceMarketDataAPIUtils:
    symbol_all = []
    symbol_info = dict()

    def __init__(self):

        self.base_url = 'https://fapi.binance.com'

        self.client = httpx.Client(proxies=global_config.proxies)
        self.api_cache = Cache(config.api_cache_dir)
        self.request_weight_limit = 2000  # binance limit 2400
        self.request_weight_cache = Cache(f'{global_config.cache_dir}/binance_request_weight')
        self.kline_expire = 32 * 24 * 60 * 60
        self.md_ws_cache_dir = global_config.md_ws_cache
        self.num_workers = config.num_threads
        self.executor = ThreadPoolExecutor(max_workers=self.num_workers)

        self.init_exchange_info()

    def __del__(self):
        self.client.close()

    def get_request_weight_limit(self):
        return sum(self.request_weight_cache.get(k, 0) for k in self.request_weight_cache.iterkeys())

    def check_rate_limit(self):
        if self.request_weight_cache.get('api_limit') is not None:
            return False
        if self.get_request_weight_limit() > self.request_weight_limit:
            return False
        return True

    def get(self, url, params=None, use_cache=False, retry_times=0) -> dict:

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
        try:
            response: Response = self.client.get(url, params=params)
        except ConnectTimeout:
            logger.error(f"GET Request failed with ConnectTimeout. url: {url}, param: {params}, retrying...")
            if retry_times < 5:
                return self.get(url, params=params, use_cache=use_cache, retry_times=retry_times + 1)
            else:
                return {}

        if response.status_code == 200:
            resp = response.json()
            self.api_cache[cache_key] = resp
            # read rate limit from header
            text = f"GET request successful. " \
                   f"limit: {self.get_request_weight_limit()}, url: {url}, param: {params}, resp: {resp}."
            if len(text) > 512:
                text = text[:512]
            logger.info(text)
            return resp
        elif response.status_code == 429:
            self.api_cache[cache_key] = None
            logger.error(f"GET Request failed with status code: {response.status_code}. url: {url}, param: {params}.")
            logger.error(f"RATE LIMIT 429 !!!")
            self.request_weight_cache.set('api_limit', 0, expire=60)
        else:
            self.api_cache[cache_key] = None
            logger.error(f"GET Request failed with status code: {response.status_code}. url: {url}, param: {params}.")

    def get_server_time(self):
        """
        API 获取服务器时间
        :return: "serverTime": 1499827319559
        """
        self.request_weight_cache.push(1, expire=60)
        return self.get("/fapi/v1/time")

    def check_delay(self, retry_times=3):
        delay_ls = [- (self.get_server_time()["serverTime"] - get_ms()) for _ in range(retry_times)]
        return sum(delay_ls) / len(delay_ls), max(delay_ls)

    def get_exchange_info(self, use_cache=False):
        """
        API 获取交易规则和交易对
        """
        self.request_weight_cache.push(1, expire=60)
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
        self.request_weight_cache.push(1, expire=60)

        return self.get(
            "/fapi/v1/premiumIndex",
            params=dict(
                symbol=symbol,
            )
        )

    def get_klines(self, symbol, interval, start_time=None, end_time=None, limit=None):
        """
        API K 线数据

        :param symbol:
        :param interval:
        :param startTime:
        :param endTime:
        :param limit:
        :return:
        """
        if limit is None:
            raise NotImplementedError("limit must be set")

        if limit < 100:
            self.request_weight_cache.push(1, expire=60)
        elif limit < 500:
            self.request_weight_cache.push(2, expire=60)
        elif limit <= 1000:
            self.request_weight_cache.push(5, expire=60)
        else:
            self.request_weight_cache.push(10, expire=60)

        return self.get(
            "/fapi/v1/klines",
            params=dict(
                symbol=symbol,
                interval=interval,
                startTime=start_time,
                endTime=end_time,
                limit=limit
            )
        )

    def _fix_history_kline_worker(self, symbols, interval, event, limit, hours):

        for symbol in symbols:

            symbol = symbol.lower()
            timestamps = get_timestamp_list_hour(hours, limit)
            cache = Cache(f'{self.md_ws_cache_dir}/{symbol}@{event}')

            ori_lines = len(cache)
            klines = self.get_klines(
                symbol, interval, timestamps[0], timestamps[-1], limit
            )

            for kline in klines:
                key = kline[0]
                if cache.get(key) is None or not cache[key][-1]:
                    value = [
                        kline[0],
                        kline[0],
                        kline[0],
                        kline[6],
                        float(kline[1]),
                        float(kline[2]),
                        float(kline[3]),
                        float(kline[4]),
                        float(kline[5]),
                        float(kline[7]),
                        int(kline[8]),
                        float(kline[9]),
                        float(kline[10]),
                        True
                    ]
                    cache.set(key, value, expire=self.kline_expire - (datetime.now().timestamp() - key / 1000))

            logger.info(f"Fix cache {symbol}@{event}, fix {len(cache) - ori_lines} lines")

    def _fix_history_kline(self, interval, event, limit, hours):

        timestamps = get_timestamp_list_hour(hours, limit)

        symbol_all = []
        for symbol in self.symbol_all:
            symbol = symbol.lower()
            cache = Cache(f'{self.md_ws_cache_dir}/{symbol}@{event}')
            if all(cache.get(t) is not None and cache[t][-1] for t in timestamps):
                logger.info(f"Fixed cache {symbol}@{event}, skip ...")
            else:
                symbol_all.append(symbol)

        if len(symbol_all) == 0:
            logger.info(f"Fix all klines {interval} cache (nothing to do).")
            return

        # performance depends on split length
        split_symbols = split_list_by_length(symbol_all, 12)

        wait([
            self.executor.submit(
                self._fix_history_kline_worker, symbols, interval, event, limit, hours
            ) for symbols in split_symbols
        ])

        logger.info(f"Fix all klines {interval} cache with {len(split_symbols)} group workers.")

    def fix_history_kline_1h(self):
        self._fix_history_kline(interval='1h', event='kline_1h', limit=24 * 7, hours=1)

    def fix_history_kline_8h(self):
        self._fix_history_kline(interval='8h', event='kline_8h', limit=3 * 24, hours=8)


if __name__ == '__main__':
    import time

    FT = BinanceMarketDataAPIUtils()
    FT.get_server_time()
    # s.get_all_history_order()
    # s.get_balance()
    # print(s.symbol_info['ETHUSDT'])
    # print(s.symbol_all)

    time.sleep(10)
