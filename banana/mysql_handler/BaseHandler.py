from peewee import Model


future_usdt_symbol_all = [f'{x}USDT'.lower() for x in [
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


kline_list = [
    'kline1m',
    'kline1h'
    'kline8h',
]


class BaseStreamMysqlHandler:
    date: str
    symbol: str
    event: str

    def __init__(self, symbol, event):
        self.symbol = symbol
        self.event = event

    def on_close(self):
        pass

    def process_line(self, data, rec_time):
        self._process_line(data, rec_time)

    def _process_line(self, data, rec_time):
        raise NotImplementedError


def generate_models(table_names, meta_type):
    models = {}
    for table_name in table_names:
        # 动态创建Model类
        model_name = table_name.capitalize()
        model_meta = type('Meta', (object,), {'table_name': table_name})
        model_attrs = {
            'Meta': model_meta,
            '__module__': __name__
        }
        model: Model | meta_type | type = type(model_name, (meta_type,), model_attrs)
        models[table_name] = model

        if not model.table_exists():
            print(f"{model} not exists, try create {table_name} in {model._meta.database.database}")
            model.create_table()

    return models
