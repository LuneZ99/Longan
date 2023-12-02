import pandas as pd
import numpy as np
from diskcache import Cache
from tools import global_config


def get_md_cache(
        symbol=None,
        event=None,
        cache=None,
        cols=None,
        cache_path=global_config.future_md_ws_cache,
        array_type='dataframe',
        apply_fn=None,
        apply_inplace=True,
        *apply_args
):
    cache = cache or Cache(f'{cache_path}/{symbol}@{event}')
    lst = []
    for k in cache.iterkeys():
        if temp := cache.get(k):
            lst.append(temp)
    if array_type == 'dataframe':
        res = pd.DataFrame(lst)
        if cols:
            res.columns = cols
    elif array_type == 'ndarray':
        res = np.array(lst)
    elif array_type == 'ndarray_dict':
        raise NotImplementedError
    else:
        res = lst

    if apply_fn is not None:
        if apply_inplace:
            apply_fn(res, *apply_args)
        else:
            res = apply_fn(res, *apply_args)

    return res


def pre_process_kline_df(df):
    df['finish_pct'] = 1.0
    df['count'] = df['count'].astype(float)
    df_nf = df.loc[~df['finish']]
    df.loc[~df['finish'], 'finish_pct'] = (df_nf['event_time'] - df_nf['open_time'] - 3002.0) / (
                df_nf['close_time'] - df_nf['open_time'])
    for col in ['volume', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume']:
        df.loc[~df['finish'], col] /= df.loc[~df['finish'], 'finish_pct']
