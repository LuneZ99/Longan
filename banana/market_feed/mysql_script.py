from mysql_adapter import Kline1m, Kline8h



if __name__ == '__main__':
    import pandas as pd

    df = pd.read_feather('data.fea')

    line = df.iloc[0].to_dict()
    Kline1m.create(**line)

    pass


