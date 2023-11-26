critical_error_code = [
    -1002, -1003, -1006, -1007, -1008, -1014, -1015, -1016, -1020, -1021, -1022, -1023, -1099,
    -1100, -1101, -1102, -1103, -1104, -1105, -1106, -1108, -1109, -1110, -1112, -1113, -1114,
    -1115, -1116, -1117, -1118, -1119, -1120, -1121, -1122, -1125, -1127, -1128, -1130, -1136,

]


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
    bv1 = 6
    bp2 = 7
    bv2 = 8
    bp3 = 9
    bv3 = 10
    bp4 = 11
    bv4 = 12
    bp5 = 13
    bv5 = 14
    bp6 = 15
    bv6 = 16
    bp7 = 17
    bv7 = 18
    bp8 = 19
    bv8 = 20
    bp9 = 21
    bv9 = 22
    bp10 = 23
    bv10 = 24
    bp11 = 25
    bv11 = 26
    bp12 = 27
    bv12 = 28
    bp13 = 29
    bv13 = 30
    bp14 = 31
    bv14 = 32
    bp15 = 33
    bv15 = 34
    bp16 = 35
    bv16 = 36
    bp17 = 37
    bv17 = 38
    bp18 = 39
    bv18 = 40
    bp19 = 41
    bv19 = 42
    bp20 = 43
    bv20 = 44
    sp1 = 45
    sv1 = 46
    sp2 = 47
    sv2 = 48
    sp3 = 49
    sv3 = 50
    sp4 = 51
    sv4 = 52
    sp5 = 53
    sv5 = 54
    sp6 = 55
    sv6 = 56
    sp7 = 57
    sv7 = 58
    sp8 = 59
    sv8 = 60
    sp9 = 61
    sv9 = 62
    sp10 = 63
    sv10 = 64
    sp11 = 65
    sv11 = 66
    sp12 = 67
    sv12 = 68
    sp13 = 69
    sv13 = 70
    sp14 = 71
    sv14 = 72
    sp15 = 73
    sv15 = 74
    sp16 = 75
    sv16 = 76
    sp17 = 77
    sv17 = 78
    sp18 = 79
    sv18 = 80
    sp19 = 81
    sv19 = 82
    sp20 = 83
    sv20 = 84

    COLS = (
            ['update_id', 'prev_update_id', 'rec_time', 'event_time', 'trade_time'] +
            sum(([f"bp{i}", f"bv{i}"] for i in range(1, 21)), []) +
            sum(([f"sp{i}", f"sv{i}"] for i in range(1, 21)), [])
    )


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
