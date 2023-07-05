from pydantic import BaseSettings
from typing import List, Optional
import yaml


class BinanceConfig(BaseSettings):
    strategy_name: str = ""

    @classmethod
    def load_from_yaml(cls, file_path: str):
        with open(file_path, 'r') as f:
            yaml_data = yaml.safe_load(f)
        return cls(**yaml_data)
