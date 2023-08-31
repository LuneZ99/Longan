from diskcache import Cache, FanoutCache
import multiprocessing


def create_cache():
    cache_folder = "/mnt/0/tmp/binance_cache"
    cache_path = f"{cache_folder}/kline_1h"
    dc = FanoutCache(cache_path, timeout=0.1)
    print(dc)


processes = []
p = multiprocessing.Process(target=create_cache)
p.daemon = True
p.start()
processes.append(p)
