import os
from dataclasses import dataclass

import yaml

from tools import get_logger, get_config


@dataclass
class Config:
    api_key: str
    api_secret: str

    @classmethod
    def from_yaml(cls, file_path: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")):
        with open(file_path, 'r') as file:
            return cls(**yaml.safe_load(file))

    @classmethod
    def from_nacos(cls):
        return cls(**get_config("binance_td.future", "longan"))


# config = Config.from_yaml(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"))
config = Config.from_nacos()

logger = get_logger("future_td_ws")

if __name__ == '__main__':
    print(config)
