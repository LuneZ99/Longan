import logging
import asyncio


class BinanceAsyncLogger:
    def __init__(self, log_file):
        self.log_file = log_file
        self.queue = asyncio.Queue()
        self.worker_task = None

        self.logger = logging.getLogger('async_logger')
        self.logger.setLevel(logging.INFO)

        handler = logging.FileHandler(log_file)
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


if __name__ == "__main__":
    # 使用示例
    async def main():
        logger = BinanceAsyncLogger('log.txt')
        await logger.log(logging.INFO, 'This is an async log message')
        await logger.log(logging.ERROR, 'This is another async log message')
        await logger.close()

    asyncio.run(main())
