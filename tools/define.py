class MsgType:
    register = 'r'
    broadcast = 'b'
    subscribe = 's'


class RegisterType:
    sender = 's'
    receiver = 'r'


class TimeInForce:
    GTC = 'GTC'
    IOC = 'IOC'
    FOK = 'FOK'
    GTX = 'GTX'
    GTD = 'GTD'


class OrderStatus:
    NOT_SEND = 'NOT_SEND'

    NEW = 'NEW'
    PARTIALLY_FILLED = 'PARTIALLY_FILLED'
    FILLED = 'FILLED'
    CANCELED = 'CANCELED'
    REJECTED = 'REJECTED'
    EXPIRED = 'EXPIRED'
    EXPIRED_IN_MATCH = 'EXPIRED_IN_MATCH'


class OrderType:
    LIMIT = 'LIMIT'
    MARKET = 'MARKET'
    STOP = 'STOP'
    STOP_MARKET = 'STOP_MARKET'
    TAKE_PROFIT = 'TAKE_PROF'
    TAKE_PROFIT_MARKET = 'TAKE_PROFIT_MARKET'
    TRAILING_STOP_MARKET = 'TRAILING_STOP_MARKET'


class OrderSide:
    BUY = 'BUY'
    SELL = 'SELL'


class OrderPosition:
    BOTH = 'BOTH'
    LONG = 'LONG'
    SHORT = 'SHORT'
