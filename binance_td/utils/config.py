from tools.dot_dict import DotDict


class Config(DotDict):
    api_key: str
    api_secret: str
    proxy_url: str

config = Config.from_yaml('utils/config.yaml')
# print(config.disk_cache_folder)


if __name__ == '__main__':
    print(config)
