import multiprocessing
import signal
import time

from banana.market_feed.md2sql import BinanceFutureMD


def md2sql_worker(
        worker_name, symbols_all, subscribe_list,
        log_interval=10000, proxy="http://127.0.0.1:7890", ws_trace=False
):

    s = BinanceFutureMD(
        log_file=f"{worker_name}.log",
        symbols=symbols_all,
        proxy=proxy,
        ws_trace=ws_trace
    )

    for _symbol in symbols_all:
        for md in subscribe_list:
            s.subscribe(_symbol, md, log_interval=log_interval)

    s.run()


def split_list(lst, num_parts):
    result = [[] for _ in range(num_parts)]
    for i, num in enumerate(lst):
        index = i % num_parts
        result[index].append(num)
    return result


def signal_handler(signum, frame):
    print("接收到信号，开始关闭子进程和主进程")
    for p in multiprocessing.active_children():
        p.terminate()
        p.join()
    exit(0)



if __name__ == '__main__':

    signal.signal(signal.SIGINT, signal_handler)

    symbols = [f'{x}USDT'.lower() for x in [
        '1000FLOKI', '1000LUNC', '1000PEPE', '1000SHIB', '1000XEC', '1INCH', 'AAVE', 'ACH', 'ADA', 'AGIX', 'AGLD',
        'ALGO', 'ALICE', 'ALPHA', 'AMB', 'ANKR', 'ANT', 'APE', 'API3', 'APT', 'ARB', 'ARKM', 'ARPA', 'AR', 'ASTR',
        'ATA', 'ATOM', 'AUDIO', 'AVAX', 'AXS', 'BAKE', 'BAL', 'BAND', 'BAT', 'BCH', 'BEL', 'BLUEBIRD', 'BLUR', 'BLZ',
        'BNB', 'BNT', 'BNX', 'BTCDOM', 'BTC', 'C98', 'CELO', 'CELR', 'CFX', 'CHR', 'CHZ', 'CKB', 'COMBO', 'COMP',
        'COTI', 'CRV', 'CTK', 'CTSI', 'CVX', 'CYBER', 'DAR', 'DASH', 'DEFI', 'DENT', 'DGB', 'DODOX', 'DOGE', 'DOT',
        'DUSK', 'DYDX', 'EDU', 'EGLD', 'ENJ', 'ENS', 'ETC', 'ETH', 'FET', 'FIL', 'FLOW', 'FOOTBALL', 'FTM', 'FXS',
        'GALA', 'GAL', 'GMT', 'GMX', 'GRT', 'GTC', 'HBAR', 'HFT', 'HIGH', 'HOOK', 'HOT', 'ICP', 'ICX', 'IDEX', 'ID',
        'IMX', 'INJ', 'IOST', 'IOTA', 'IOTX', 'JASMY', 'JOE', 'KAVA', 'KEY', 'KLAY', 'KNC', 'KSM', 'LDO', 'LEVER',
        'LINA', 'LINK', 'LITU', 'LPTU', 'LQTY', 'LRC', 'LTC', 'LUNA2', 'MAGIC', 'MAMA', 'MASK', 'MATIC', 'MAV',
        'MDT', 'MINA', 'MKR', 'MTL', 'NEAR', 'NEO', 'NKN', 'NMR', 'OCEAN', 'OGN', 'OMG', 'ONE', 'ONT', 'OP', 'OXT',
        'PENDLE', 'PEOPLE', 'PERP', 'PHB', 'QNT', 'QTUM', 'RAD', 'RDNT', 'REEF', 'REN', 'RLC', 'RNDR', 'ROSE',
        'RSR', 'RUNE', 'RVN', 'SAND', 'SEI', 'SFP', 'SKL', 'SNX', 'SOL', 'SPELL', 'SSV', 'STG', 'STMX', 'STORJ', 'STX',
        'SUI', 'SUSHI', 'SXP', 'THETA', 'TLM', 'TOMO', 'TRB', 'TRU', 'TRX', 'T', 'UMA', 'UNFI', 'UNI', 'VET', 'WAVES',
        'WLD', 'WOO', 'XEM', 'XMR', 'XLM', 'XRP', 'XTZ', 'XVG', 'XVS', 'YFI', 'YGG', 'ZEC', 'ZEN', 'ZIL', 'ZRX'
    ]]

    subscribe_list_all = [
        'kline_1m', 'kline_1h', 'kline_8h', 'aggTrade', 'bookTicker', 'depth20'
    ]

    split_num = len(symbols) * len(subscribe_list_all) // 200 + 1

    split_symbols = split_list(symbols, split_num)

    for symbol in split_symbols:
        print(len(symbol))

    md2sql_worker(f"MD1", split_symbols[0], subscribe_list_all)

    # print(len(symbols), len(subscribe_list_all), len(symbols) * len(subscribe_list_all))


    # processes = list()
    # for param in parameters:
    #     p = multiprocessing.Process(target=worker, args=(param,))
    #     p.daemon = True
    #     p.start()
    #     processes.append(p)
    #
    # p = multiprocessing.Process(target=md2sql_worker, args=("name",))
    # p.daemon = True
    # p.start()
    #
    # while True:
    #     time.sleep(1)

