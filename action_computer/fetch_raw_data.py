import pandas as pd

from mysql_handler import *

from datetime import datetime, timedelta

query = models_kline['kline1m'].select().where(
    models_kline['kline1m'].symbol == "1000flokiusdt"
)



df = pd.DataFrame(list(query.dicts()))

print(df.iloc[-1]['close_time']+1)
print(datetime.fromtimestamp((df.iloc[-1]['close_time']+1) / 1000))
# 1693159725654
# 1693152000


def get_timestamps():

    timestamps = []
    current_time = datetime.now()

    print(int(current_time.timestamp() * 1000))
    print(current_time)

    hour = current_time.hour  # 获取当前小时数

    # 计算最近一个整数小时的时间戳
    if hour < 8:
        target_hour = 0
    elif hour < 16:
        target_hour = 8
    else:
        target_hour = 16

    current_time = current_time.replace(hour=target_hour, minute=0, second=0, microsecond=0)

    interval = timedelta(hours=8)

    for _ in range(16):
        current_time -= interval
        current_time = current_time.replace(hour=0, minute=0, second=0)
        timestamps.append(int(current_time.timestamp() * 1000))

    return timestamps

timestamps = get_timestamps()
print(timestamps)