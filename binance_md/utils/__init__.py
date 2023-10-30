from .config import config
from .logger import BinanceSyncLogger
from .tools import *


logger_md = BinanceSyncLogger(f"{config.cache_folder}/logs/log.binance_md_ws")
