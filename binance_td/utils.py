import os
from dataclasses import dataclass

import yaml

from tools import get_logger, global_config


@dataclass
class Config:
    api_key: str
    api_secret: str

    @classmethod
    def from_yaml(cls, file_path: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")):
        with open(file_path, 'r') as file:
            return cls(**yaml.safe_load(file))


config = Config.from_yaml(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"))

logger = get_logger("logger_td_ws", f"{global_config}/log.binance_td_ws")


if __name__ == '__main__':
    print(config)
