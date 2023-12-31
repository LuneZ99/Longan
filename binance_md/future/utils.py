import os
from dataclasses import dataclass

import yaml
from peewee import Model

from tools import get_logger, get_config


@dataclass
class Config:
    num_threads: int
    mysql: dict
    # future_symbols: list[str]
    kline_list: list[str]
    subscribe_events: list[str]
    event_push_to_litchi_md: list[str]
    push_to_litchi: bool

    @classmethod
    def from_yaml(cls, file_path: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")):
        with open(file_path, 'r') as file:
            return cls(**yaml.safe_load(file))

    @classmethod
    def from_nacos(cls):
        return cls(**get_config("binance_md.future", "longan"))


# config = Config.from_yaml(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"))

config = Config.from_nacos()

logger = get_logger("future_md_ws")


def generate_models(table_names, meta_type):
    models = {}
    for table_name in table_names:
        # 动态创建Model类
        model_name = table_name.capitalize()
        model_meta = type('Meta', (object,), {'table_name': table_name})
        model_attrs = {
            'Meta': model_meta,
            '__module__': __name__
        }
        model: Model | meta_type | type = type(model_name, (meta_type,), model_attrs)
        models[table_name] = model
        # 确保连接到数据库
        # model._meta.database.connect()
        # 如果表不存在，创建它
        model.create_table(safe=True)

    return models


if __name__ == '__main__':
    print(config)
