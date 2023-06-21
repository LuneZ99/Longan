from .BaseConfig import BinanceConfig
from .BaseLogger import BinanceAsyncLogger


class BinanceStrategy:

    def __init__(self, log_file="log.default.txt", config_file=None, proxy=None):

        self.config = BinanceConfig()
        if config_file is not None:
            self.config = self.config.load_from_yaml(config_file)

        self.log_file = BinanceAsyncLogger(log_file)

        pass

    def subscribe_ws(self):
        pass


if __name__ == '__main__':
    s = BinanceStrategy()





