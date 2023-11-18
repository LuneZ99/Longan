import os
from dataclasses import dataclass
from peewee import Model
import yaml

from tools import get_logger, global_config


@dataclass
class Config:
    num_threads: int
    mysql: dict
    future_symbols: list[str]
    kline_list: list[str]
    subscribe_events: list[str]
    event_push_to_litchi_md: list[str]
    push_to_litchi: bool

    @classmethod
    def from_yaml(cls, file_path: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")):
        with open(file_path, 'r') as file:
            return cls(**yaml.safe_load(file))


config = Config.from_yaml(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"))
# print(config)

logger = get_logger("logger_future_md", f"{global_config}/log.binance_future_md")


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

