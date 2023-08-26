import os
from datetime import datetime
from typing import Optional, IO

import pytz


class BaseStreamCsvHandler:
    date: str
    symbol: str
    event: str
    handle: Optional[IO]
    headers: str
    file_name: str

    def __init__(self, path, symbol, event):
        self.parent_path = path
        self.symbol = symbol
        self.event = event
        self.handle = None

        now = datetime.now()
        utc_now = now.astimezone(pytz.utc)
        utc_date = str(utc_now)[:10]
        self.date = utc_date
        self.flush_count = 0

    def on_close(self):
        if self.handle:
            self.handle.close()

    def process_line(self, info):

        now = datetime.now()
        utc_now = now.astimezone(pytz.utc)
        utc_date = str(utc_now)[:10]
        if self.date < utc_date:
            self.date = utc_date
            self._reset_handle()

        line = self._process_line(info)
        if len(line) > 0:
            self.handle.write("\n" + ",".join(map(str, line)))
            self.handle.flush()
            # self.flush_count += 1
            # if self.flush_count % 5000 == 0:
            #     self.handle.flush()

    def _reset_handle(self):

        if self.handle:
            self.handle.close()

        self.file_name = f"{self.parent_path}/{self.symbol}/{self.date}/{self.event}.csv"

        if not os.path.exists(f"{self.parent_path}/{self.symbol}/{self.date}"):
            os.makedirs(f"{self.parent_path}/{self.symbol}/{self.date}")

        if os.path.exists(self.file_name):
            if os.path.getsize(self.file_name) <= 1024:
                self.handle = open(self.file_name, "w")
                self._write_csv_header()
            else:
                self.handle = open(self.file_name, "a")
        else:
            self.handle = open(self.file_name, "w")
            self._write_csv_header()

    def _write_csv_header(self):
        self.handle.write(self.headers)
        self.handle.flush()

    def _process_line(self, info):
        raise NotImplementedError