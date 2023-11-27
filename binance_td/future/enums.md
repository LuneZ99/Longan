# 枚举定义

交易对类型:

- FUTURE 期货 

合约类型 (contractType):

- PERPETUAL 永续合约
- CURRENT_MONTH 当月交割合约
- NEXT_MONTH 次月交割合约
- CURRENT_QUARTER 当季交割合约
- NEXT_QUARTER 次季交割合约
- PERPETUAL_DELIVERING 交割结算中合约

合约状态 (contractStatus,** **status):

- PENDING_TRADING 待上市
- TRADING 交易中
- PRE_DELIVERING 预交割
- DELIVERING 交割中
- DELIVERED 已交割
- PRE_SETTLE 预结算
- SETTLING 结算中
- CLOSE 已下架

订单状态 (status):

- NEW 新建订单
- PARTIALLY_FILLED 部分成交
- FILLED 全部成交
- CANCELED 已撤销
- REJECTED 订单被拒绝
- EXPIRED 订单过期(根据timeInForce参数规则)
- EXPIRED_IN_MATCH 订单被STP过期

订单种类 (orderTypes, type):

- LIMIT 限价单
- MARKET 市价单
- STOP 止损限价单
- STOP_MARKET 止损市价单
- TAKE_PROFIT 止盈限价单
- TAKE_PROFIT_MARKET 止盈市价单
- TRAILING_STOP_MARKET 跟踪止损单

订单方向 (side):

- BUY 买入
- SELL 卖出

持仓方向:

- BOTH 单一持仓方向
- LONG 多头(双向持仓下)
- SHORT 空头(双向持仓下)

有效方式 (timeInForce):

- GTC - Good Till Cancel 成交为止
- IOC - Immediate or Cancel 无法立即成交(吃单)的部分就撤销
- FOK - Fill or Kill 无法全部立即成交就撤销
- GTX - Good Till Crossing 无法成为挂单方就撤销
- GTD - Good Till Date 在特定时间之前有效，到期自动撤销 

条件价格触发类型 (workingType)

- MARK_PRICE
- CONTRACT_PRICE

响应类型 (newOrderRespType)

- ACK
- RESULT

K线间隔:

m -> 分钟; h -> 小时; d -> 天; w -> 周; M -> 月

- 1m
- 3m
- 5m
- 15m
- 30m
- 1h
- 2h
- 4h
- 6h
- 8h
- 12h
- 1d
- 3d
- 1w
- 1M

 
防止自成交模式:

- NONE
- EXPIRE_TAKER
- EXPIRE_BOTH
- EXPIRE_MAKER

盘口价下单模式:

- OPPONENT (盘口对手价)
- OPPONENT_5 (盘口对手5档价)
- OPPONENT_10 (盘口对手10档价)
- OPPONENT_20
- QUEUE (盘口同向价)
- QUEUE_5 (盘口同向排队5档价)
- QUEUE_10 (盘口同向排队10档价)
- QUEUE_20 (盘口同向排队20档价)- a