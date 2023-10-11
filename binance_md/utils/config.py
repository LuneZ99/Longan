from tools.dot_dict import DotDict


class Config(DotDict):
    future_symbols: list
    disk_cache_folder: str


config = Config.from_yaml('utils/config.yaml')
# print(config.disk_cache_folder)
config.future_symbols = [f'{x}'.lower() for x in config.future_symbols]


if __name__ == '__main__':
    print(config)
