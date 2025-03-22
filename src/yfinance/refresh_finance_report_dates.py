import time

import pandas as pd
import yfinance as yf

from src.symbols import symbols
from src.utils.path_utils import get_raw_path, get_root_path, get_latest_date, get_quandl_path, get_data_path


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


def refresh_finance_report_dates():
    last_csv_name = get_latest_date(get_quandl_path('option_iv_raw'))
    df = pd.read_csv(get_quandl_path(f'option_iv_raw/{last_csv_name}.csv'))
    symbols = sorted(df['ticker'].astype(str).unique())
    with open(get_data_path('financeReportDate.csv'), "w") as file:
        for symbol in symbols:
            try:
                data_str = get_earning_data(symbol)
                file.write(symbol + ',' +data_str + '\n')
                file.flush()
            except Exception as e:
                print(symbol)
                print(e)
                print('---------')
            finally:
                time.sleep(0.2)



if __name__ == '__main__':
    refresh_finance_report_dates()
