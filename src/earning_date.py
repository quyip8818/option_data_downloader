import time

import pandas as pd
import yfinance as yf

from src.utils.path_utils import get_raw_path


def localize_date(date):
    if date.tz is None:
        date = date.tz_localize('UTC')
    return date.tz_convert('US/Eastern').date.astype(str)


def get_earning_data(symbol):
    ticker = yf.Ticker(symbol)
    df = ticker.get_earnings_dates(limit=100)
    df = df[df['Reported EPS'].notna()]
    date= localize_date(df.index)
    return '|'.join(date)


df = pd.read_csv(get_raw_path('symbols.csv'))
# processed_df = pd.read_csv('raw/symbols.csv')

with open("finance/all.csv", "w") as file:
    for symbol in df['symbols'].unique():
        try:
            data_str = get_earning_data(symbol)
            file.write(symbol + ',' +data_str + '\n')
            file.flush()
        except Exception as e:
            print(symbol)
            print(e)
            print('---------')
        finally:
            time.sleep(1)
