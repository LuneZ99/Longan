from dataclasses import dataclass
import os
from pathlib import Path
import logging
import yaml
from logging.handlers import TimedRotatingFileHandler

from diskcache import Cache


@dataclass
class GlobalConfig:
    log_dir: str
    rate_limit_cache: str

    future_cache_dir: str
    future_disk_cache_dir: str
    future_md_ws_cache: str
    future_local_order_cache: str
    future_flag_dir: str

    proxies: dict[str, str]
    proxy_url: str

    litchi_md_url: str

    @classmethod
    def from_yaml(cls, file_path: str):
        with open(file_path, 'r') as file:
            return cls(**yaml.safe_load(file))


global_config = GlobalConfig.from_yaml(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"))


def get_logger(name, logger_dir=global_config.log_dir, log_console=True, level=logging.INFO) -> logging.Logger:

    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s | %(message)s')

    logger_path = Path(logger_dir) / name
    if not os.path.exists(logger_path):
        os.makedirs(logger_path)
    file_handler = TimedRotatingFileHandler(
        filename=logger_path / "log",
        when='midnight', interval=1, backupCount=180
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if log_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


global_logger = get_logger("global_future")


class RateLimit:
    def __init__(self):
        self.rate_limit_used = Cache(f'{global_config.rate_limit_cache}')
        self.rate_limit = {
            'x-mbx-used-weight-1m': 2200,  # 2400
            'x-mbx-order-count-10s': 280,  # 300
            'x-mbx-order-count-1m': 1100,  # 1200
        }
        for key in self.rate_limit.keys():
            if key not in self.rate_limit_used:
                self.rate_limit_used[key] = 0

    def update(self, headers):
        for key in self.rate_limit.keys():
            if key in headers.keys():
                self.rate_limit_used[key] = int(headers[key])

    def is_limited(self):
        limited = any(self.rate_limit_used[key] > self.rate_limit[key] for key in self.rate_limit.keys())
        if limited:
            rate_limit_used = {
                self.rate_limit_used[key] for key in self.rate_limit.keys()
            }
            global_logger.warning(
                f"API access rate exceeds limit !!! {rate_limit_used}"
            )
        return limited


rate_limit = RateLimit()

