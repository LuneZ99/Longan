from utils.dot_dict import DotDict


config = DotDict.from_yaml('/mnt/0/lune/Longan/utils/config.yaml')
print(config.disk_cache_folder)
config.usdt_future_symbol_all = [f'{x}USDT'.lower() for x in config.usdt_future_coins]


if __name__ == '__main__':
    print(config)
