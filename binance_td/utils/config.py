import os

from tools.dot_dict import DotDict


class Config(DotDict):
    api_key: str
    api_secret: str
    proxy_url: str


root_dir = os.path.dirname(os.path.abspath(__file__))
config = Config.from_yaml(os.path.join(root_dir, "config.yaml"))
# print(config.disk_cache_folder)


if __name__ == '__main__':
    print(config)
