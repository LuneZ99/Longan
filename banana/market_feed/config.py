import yaml
from pydantic import BaseModel


class BinanceConfig(BaseModel):
    strategy_name: str = ""

    @classmethod
    def load_from_yaml(cls, file_path: str):
        with open(file_path, 'r') as f:
            yaml_data = yaml.safe_load(f)
        return cls(**yaml_data)


config_md = BinanceConfig()
# config_md.load_from_yaml("config.yaml")
