from dot_dict import DotDict


config = DotDict.from_yaml('config.yaml')
config.usdt_future_symbol_all = [f'{x}USDT'.lower() for x in config.usdt_future_coins]


if __name__ == '__main__':
    print(config)
