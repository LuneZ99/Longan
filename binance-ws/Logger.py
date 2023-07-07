import asyncio
import logging
import queue
from logging.handlers import TimedRotatingFileHandler


class BinanceAsyncLogger:
    def __init__(self, log_file):
        self.log_file = log_file
        self.queue = asyncio.Queue()
        self.worker_task = None

        self.logger = logging.getLogger('async_logger')
        self.logger.setLevel(logging.INFO)

        handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=0)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    async def log(self, level, message):
        await self.queue.put((level, message))
        if not self.worker_task or self.worker_task.done():
            self.worker_task = asyncio.create_task(self._write_logs())

    async def _write_logs(self):
        while True:
            level, message = await self.queue.get()
            if isinstance(message, dict):
                message = str(message)
            self.logger.log(level, message)
            self.queue.task_done()

    async def close(self):
        if self.worker_task:
            await self.queue.join()
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
        self.logger.handlers[0].close()
        self.logger.removeHandler(self.logger.handlers[0])


class BinanceSyncLogger:
    def __init__(self, log_file):
        self.log_file = log_file
        self.queue = queue.Queue()
        self.worker_task = None

        self.logger = logging.getLogger('sync_logger')
        self.logger.setLevel(logging.INFO)

        handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=0)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def log(self, level, message):
        self.queue.put((level, message))
        if not self.queue.empty():
            self._write_logs()

    def _write_logs(self):
        while not self.queue.empty():
            level, message = self.queue.get()
            if isinstance(message, dict):
                message = str(message)
            self.logger.log(level, message)
            self.queue.task_done()

    def close(self):
        if self.worker_task:
            self.queue.join()
            self.worker_task.cancel()
            try:
                self.worker_task
            except asyncio.CancelledError:
                pass
        self.logger.handlers[0].close()
        self.logger.removeHandler(self.logger.handlers[0])


if __name__ == "__main__":
    # 使用示例
    async def main():
        logger = BinanceAsyncLogger('log.txt')
        await logger.log(logging.INFO, 'This is an async log message')
        await logger.log(logging.ERROR, 'This is another async log message')
        await logger.close()


    asyncio.run(main())
