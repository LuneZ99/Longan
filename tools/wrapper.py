import os
import logging
from tools.dot_dict import DotDict


def get_logger(name, logger_dir=None, level=logging.INFO) -> logging.Logger:

    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s | %(message)s')

    if logger_dir is not None:
        file_handler = logging.FileHandler(logger_dir)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def get_config(config_dir):
    return DotDict.from_yaml(config_dir)


global_config = get_config(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"))
