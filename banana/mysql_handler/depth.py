# from peewee import *
# from .BaseHandler import future_usdt_symbol_all, generate_models, BaseStreamDiskCacheMysqlHandler
#
#
# # 设置 MySQL 数据库连接
# db = MySQLDatabase(
#     'binance_agg_trade',
#     user='root',
#     password='**REMOVED**',
#     host='**REMOVED**',
#     port=10003
# )
#
#
# class BaseDepth(Model):
#     agg_trade_id = BigIntegerField(primary_key=True, null=False)
#     rec_time = BigIntegerField(null=False)
#     event_time = BigIntegerField(null=False)
#     price = FloatField(null=False)
#     quantity = FloatField(null=False)
#     first_trade_id = BigIntegerField(null=False)
#     last_trade_id = BigIntegerField(null=False)
#     transact_time = BigIntegerField(null=False)
#     is_buyer_maker = BooleanField(null=False)
#
#     class Meta:
#         database = db
#         indexes = (
#             (('agg_trade_id',), True),
#         )
#
#
# models_agg_trades = generate_models(future_usdt_symbol_all, BaseAggTrade)
#
#
# class Depth20Handler(BaseStreamDiskCacheMysqlHandler):
#
#     def __init__(self, symbol, event='depth20', expire_time=180):
#         super().__init__(symbol, event, expire_time)
#         self.model = models_agg_trades[self.symbol]
#
#     def _process_line(self, data, rec_time):
#         return dict(
#             agg_trade_id=data['a'],
#             rec_time=rec_time,
#             event_time=data['E'],
#             price=data['p'],
#             quantity=data['q'],
#             first_trade_id=data['f'],
#             last_trade_id=data['l'],
#             transact_time=data['T'],
#             is_buyer_maker=data['m']
#         )
