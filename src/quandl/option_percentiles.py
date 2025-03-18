import os
from time import sleep

import pandas as pd

from src.quandl.headers import IvMeanHeaders, IvCallHeaders, IvPutHeaders
from src.utils.path_utils import get_quandl_option_iv_percentiles_path, get_quandl_option_iv_raw_path, \
    get_quandl_option_iv_rank_path, get_quandl_option_iv_rank_latest
from src.utils.idx_utils import get_percentile_rank
from src.utils.download_utils import download_file, get_quandl_last_day_iv_url


def find_percentiles(df, header):
    values_s = df.get(header)
    if values_s is None:
        return  None
    values_s = values_s.dropna()
    percentiles_df = pd.read_csv(get_quandl_option_iv_percentiles_path(header))
    percentiles_df.set_index('percentiles', inplace=True)
    symbol_ranks = {}
    for symbol, value in values_s.items():
        percentiles = percentiles_df.get(symbol)
        if percentiles is None:
            continue
        rank = get_percentile_rank(percentiles, value)
        symbol_ranks[symbol] = rank
    rank_s = pd.Series(symbol_ranks).astype(int)
    return pd.DataFrame({header: values_s, f'{header}_rank': rank_s})


def get_last_iv_rank():
    df = pd.read_csv(get_quandl_option_iv_rank_latest())
    df.set_index('symbol', inplace=True)
    return df




def fetch_option_percentiles(date):
    date_str = date.strftime("%Y-%m-%d")
    date_path = date.strftime("%Y_%m_%d")
    url = get_quandl_last_day_iv_url(date_str)
    raw_file_name =get_quandl_option_iv_raw_path(date_path)
    if os.path.exists(raw_file_name) and not pd.read_csv(raw_file_name).empty:
        return False
    download_file(url, raw_file_name)
    sleep(1)

    df = pd.read_csv(raw_file_name)
    if len(df) < 10:
        return False

    df = df[df['date'] == date_str]
    df.set_index('ticker', inplace=True)
    df.sort_index(inplace=True)

    dfs = []
    for header in IvMeanHeaders + IvCallHeaders + IvPutHeaders:
        dfs.append(find_percentiles(df, header))
    all_df = pd.concat(dfs, axis=1)
    all_df.rename_axis('symbol', inplace=True)
    all_df.sort_index(inplace=True)
    all_df.to_csv(get_quandl_option_iv_rank_path(date_path), index=True)
    sleep(1)
