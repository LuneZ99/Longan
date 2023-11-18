from dataclasses import dataclass
import os
from pathlib import Path
import logging
import yaml


def get_logger(name, logger_dir=None, level=logging.INFO) -> logging.Logger:

    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s | %(message)s')

    if logger_dir is not None:
        if not os.path.exists(Path(logger_dir).parent):
            os.makedirs(Path(logger_dir).parent)
        file_handler = logging.FileHandler(logger_dir)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


# def get_config(config_dir):
#     return DotDict.from_yaml(config_dir)


@dataclass
class GlobalConfig:
    cache_dir: str
    log_dir: str
    disk_cache_dir: str
    local_order_cache: str
    md_ws_cache: str

    # proxies_all: list[dict[str, str]]
    proxies: dict[str, str]
    proxy_url: str

    litchi_md_url: str

    # @property
    # def proxies(self) -> dict[str, str]:
    #     return self.proxies_all[0]
    #
    # @property
    # def proxy_url(self) -> str:
    #     return self.proxies_all[0]["http://"]

    @classmethod
    def from_yaml(cls, file_path: str):
        with open(file_path, 'r') as file:
            return cls(**yaml.safe_load(file))


global_config = GlobalConfig.from_yaml(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"))

