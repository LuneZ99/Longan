from enum import Enum


# class Enum(Enum):
#     def __str__(self):
#         return self.value


class TimeInForce(Enum):
    GTC = 'GTC'
    IOC = 'IOC'
    FOK = 'FOK'
    GTX = 'GTX'
    GTD = 'GTD'


class OrderStatus(Enum):
    NOT_SEND = 'NOT_SEND'

    NEW = 'NEW'
    PARTIALLY_FILLED = 'PARTIALLY_FILLED'
    FILLED = 'FILLED'
    CANCELED = 'CANCELED'
    REJECTED = 'REJECTED'
    EXPIRED = 'EXPIRED'
    EXPIRED_IN_MATCH = 'EXPIRED_IN_MATCH'


class OrderType(Enum):
    LIMIT = 'LIMIT'
    MARKET = 'MARKET'
    STOP = 'STOP'
    STOP_MARKET = 'STOP_MARKET'
    TAKE_PROFIT = 'TAKE_PROF'
    TAKE_PROFIT_MARKET = 'TAKE_PROFIT_MARKET'
    TRAILING_STOP_MARKET = 'TRAILING_STOP_MARKET'


class OrderSide(Enum):
    BUY = 'BUY'
    SELL = 'SELL'


class OrderPosition(Enum):
    BOTH = 'BOTH'
    LONG = 'LONG'
    SHORT = 'SHORT'


class KLineInt(Enum):
    pass
