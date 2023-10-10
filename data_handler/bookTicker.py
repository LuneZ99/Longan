from typing import Any

from data_handler.DiskCacheHandler import BaseStreamDiskCacheHandler


class BookTickerHandler(BaseStreamDiskCacheHandler):

    def __init__(self, symbol, event='bookTicker', expire_time=60):
        super().__init__(symbol, event, expire_time)

    def _process_line(self, data, rec_time) -> tuple[Any, dict]:
        return data['T'], dict(
            uid=data['u'],
            rec_time=rec_time,
            event_time=data['E'],
            transaction_time=data['T'],

            best_bid_price=float(data['b']),
            best_bid_qty=float(data['B']),
            best_ask_price=float(data['a']),
            best_ask_qty=float(data['A']),
        )
