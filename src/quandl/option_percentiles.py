import pandas as pd
import requests

from src.quandl.headers import IvMeanHeaders, IvCallHeaders, IvPutHeaders
from src.utils.path_utils import get_quandl_option_iv_percentiles_path, get_quandl_option_iv_raw_path, \
    get_quandl_option_iv_rank_path, get_quandl_option_iv_rank_latest
from src.utils.idx_utils import get_percentile_rank


def download_file(url, save_path):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(save_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)


def get_url(date_str):
    return f'https://data.nasdaq.com/api/v3/datatables/QUANTCHA/VOL.csv?date=${date_str}&api_key=ka3E2qaQEpR4Ps7a8kus'


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
        rank = get_percentile_rank(value, percentiles)
        symbol_ranks[symbol] = rank
    rank_s = pd.Series(symbol_ranks).astype(int)
    return pd.DataFrame({header: values_s, f'{header}_rank': rank_s})


def fetch_option_percentiles(date):
    date_str = date.strftime("%Y-%m-%d")
    date_path = date.strftime("%Y_%m_%d")

    url = get_url(date_str)
    raw_file_name =get_quandl_option_iv_raw_path(date_path)
    download_file(url, raw_file_name)
    df = pd.read_csv(raw_file_name)
    if len(df) < 10:
        return
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


def get_last_iv_rank():
    df = pd.read_csv(get_quandl_option_iv_rank_latest())
    df.set_index('symbol', inplace=True)
    return df

