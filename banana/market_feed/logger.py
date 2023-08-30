import logging


class BinanceSyncLogger:
    def __init__(self, log_file, console=True):
        self.log_file = log_file
        self.logger = logging.getLogger('sync_logger')
        self.logger.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        if console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def log(self, level, message):
        if isinstance(message, dict):
            message = str(message)
        self.logger.log(level, message)

    def close(self):
        for handler in self.logger.handlers:
            handler.close()
            self.logger.removeHandler(handler)


logger_md = BinanceSyncLogger("log/md.log")
