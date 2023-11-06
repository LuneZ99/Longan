from tools.dot_dict import DotDict


class MsgType:
    register = 'r'
    broadcast = 'b'
    subscribe = 's'
    push = 'p'


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
    ERROR = 'ERROR'

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


class DEPTH20:
    update_id = 0
    prev_update_id = 1
    rec_time = 2
    event_time = 3
    trade_time = 4
    bp1 = 5
    bp2 = 6
    bp3 = 7
    bp4 = 8
    bp5 = 9
    bp6 = 10
    bp7 = 11
    bp8 = 12
    bp9 = 13
    bp10 = 14
    bp11 = 15
    bp12 = 16
    bp13 = 17
    bp14 = 18
    bp15 = 19
    bp16 = 20
    bp17 = 21
    bp18 = 22
    bp19 = 23
    bp20 = 24
    bv1 = 25
    bv2 = 26
    bv3 = 27
    bv4 = 28
    bv5 = 29
    bv6 = 30
    bv7 = 31
    bv8 = 32
    bv9 = 33
    bv10 = 34
    bv11 = 35
    bv12 = 36
    bv13 = 37
    bv14 = 38
    bv15 = 39
    bv16 = 40
    bv17 = 41
    bv18 = 42
    bv19 = 43
    bv20 = 44
    sp1 = 45
    sp2 = 46
    sp3 = 47
    sp4 = 48
    sp5 = 49
    sp6 = 50
    sp7 = 51
    sp8 = 52
    sp9 = 53
    sp10 = 54
    sp11 = 55
    sp12 = 56
    sp13 = 57
    sp14 = 58
    sp15 = 59
    sp16 = 60
    sp17 = 61
    sp18 = 62
    sp19 = 63
    sp20 = 64
    sv1 = 65
    sv2 = 66
    sv3 = 67
    sv4 = 68
    sv5 = 69
    sv6 = 70
    sv7 = 71
    sv8 = 72
    sv9 = 73
    sv10 = 74
    sv11 = 75
    sv12 = 76
    sv13 = 77
    sv14 = 78
    sv15 = 79
    sv16 = 80
    sv17 = 81
    sv18 = 82
    sv19 = 83
    sv20 = 84

    COLS = ['update_id', 'prev_update_id', 'rec_time', 'event_time', 'trade_time'] + \
           [f"bp{i}" for i in range(1, 21)] + [f"bv{i}" for i in range(1, 21)] + \
           [f"sp{i}" for i in range(1, 21)] + [f"sv{i}" for i in range(1, 21)]


class AggTrade:
    agg_trade_id = 0
    rec_time = 1
    event_time = 2
    price = 3
    quantity = 4
    first_trade_id = 5
    last_trade_id = 6
    transact_time = 7
    is_buyer_maker = 8

    COLS = [
        'agg_trade_id', 'rec_time', 'event_time', 'price', 'quantity',
        'first_trade_id', 'last_trade_id', 'transact_time', 'is_buyer_maker'
    ]


class BookTicker:
    uid = 0
    rec_time = 1
    event_time = 2
    transaction_time = 3
    bp = 4
    bv = 5
    sp = 6
    sv = 7

    COLS = ['uid', 'rec_time', 'event_time', 'transaction_time', 'bp', 'bv', 'sp', 'sv']


class Kline:
    rec_time = 0
    event_time = 1
    open_time = 2
    close_time = 3
    open = 4
    high = 5
    low = 6
    close = 7
    volume = 8
    quote_volume = 9
    count = 10
    taker_buy_volume = 11
    taker_buy_quote_volume = 12
    finish = 13

    COLS = [
        'rec_time', 'event_time', 'open_time', 'close_time', 'open', 'high', 'low', 'close',
        'volume', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'finish'
    ]
