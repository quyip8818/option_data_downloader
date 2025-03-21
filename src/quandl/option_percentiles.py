import os
from time import sleep

import numpy as np
import pandas as pd

from src.quandl.headers import IvMeanHeaders, IvCallHeaders, IvPutHeaders, PercentiledIVHeader
from src.utils.path_utils import get_quandl_option_iv_raw_path, \
    get_quandl_option_iv_rank_path, get_quandl_option_iv_rank_latest, get_raw_path, get_quandl_path, \
    get_iv_percentiles_by_symbol_path, get_iv_percentiles_by_header
from src.utils.idx_utils import get_percentile_rank
from src.utils.download_utils import download_file, get_quandl_last_day_iv_url

TargetHeader = ['date'] + [h for header in PercentiledIVHeader for h in (header, f"{header}_rank")]

def find_percentiles(df, header):
    values_s = df.get(header)
    if values_s is None:
        return  None
    values_s = values_s.dropna()
    percentiles_df = pd.read_csv(get_iv_percentiles_by_header(header))
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


def percentile_last_day_iv_rank(raw_file_name, date_str):
    df = pd.read_csv(raw_file_name)
    if len(df) < 10:
        return None

    df = df[df['date'] == date_str]
    df.rename(columns={'ticker': 'symbol'}, inplace=True)
    df.set_index('symbol', inplace=True)
    df.sort_index(inplace=True)

    dfs = []
    for header in PercentiledIVHeader:
        dfs.append(find_percentiles(df, header))
    all_df = pd.concat(dfs, axis=1)
    all_df.sort_index(inplace=True)
    return all_df



def fetch_option_percentiles(date):
    date_str = date.strftime("%Y-%m-%d")
    date_path = date.strftime("%Y_%m_%d")
    url = get_quandl_last_day_iv_url(date_str)
    raw_file_name =get_quandl_option_iv_raw_path(date_path)
    if os.path.exists(raw_file_name) and not pd.read_csv(raw_file_name).empty:
        return False
    download_file(url, raw_file_name)
    sleep(1)
    df = percentile_last_day_iv_rank(raw_file_name, date_str)
    if df is None:
        return False
    df.to_csv(get_quandl_option_iv_rank_path(date_path), index=True, index_label='symbol')
    for symbol, row in df.iterrows():
        if symbol == 'nan':
            continue
        symbol_file_path = get_quandl_path(f"option_iv_rank_by_symbols/{symbol}.csv")
        df = pd.read_csv(symbol_file_path) if os.path.exists(symbol_file_path) else None
        has_symbol = df is not None and len(df) > 0
        if has_symbol and not df.loc[df['date'] == date_str].empty:
            continue

        row['date'] = date_str
        row_df = pd.DataFrame([row])
        row_df = row_df[TargetHeader]

        if has_symbol:
            df = pd.concat([row_df, df], ignore_index=True)
            df.sort_values(by=['date'], ascending=[False], inplace=True)
        else:
            df = row_df
        df.to_csv(symbol_file_path, index=False)


def quantiles_all_iv():
    q_dfs = {}
    for header in PercentiledIVHeader:
        q_dfs[header] = pd.read_csv(get_iv_percentiles_by_header(header))
        q_dfs[header].set_index('percentiles', inplace=True)

    df = pd.read_csv(get_raw_path(f"all_iv.csv"),
                     usecols=['ticker', 'date'] + PercentiledIVHeader)
    df.rename(columns={'ticker': 'symbol'}, inplace=True)
    df.dropna(inplace=True)

    for symbol, symbol_df in df.groupby('symbol'):
        print(symbol)
        symbol_df.sort_values(by=['date'], ascending=[False], inplace=True)
        for header in PercentiledIVHeader:
            percentiles = q_dfs[header].get(symbol)
            if percentiles is not None:
                symbol_df[header + '_rank'] = symbol_df.apply(
                    lambda row: get_percentile_rank(percentiles, row[header]),
                    axis=1)
            else:
                symbol_df[header + '_rank'] = np.nan
        symbol_df = symbol_df[TargetHeader]
        symbol_df = symbol_df.sort_values(by=['date'], ascending=[False])
        symbol_df.to_csv(get_quandl_path(f"option_iv_rank_by_symbols/{symbol}.csv"), index=False)


if __name__ == '__main__':
    quantiles_all_iv()
