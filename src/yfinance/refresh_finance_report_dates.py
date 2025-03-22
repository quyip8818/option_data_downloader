import time
import pandas as pd

from src.utils.path_utils import get_latest_date, get_quandl_path, get_data_path
from src.utils.yf_utils import get_earning_data


def refresh_finance_report_dates():
    last_csv_name = get_latest_date(get_quandl_path('option_iv_raw'))
    df = pd.read_csv(get_quandl_path(f'option_iv_raw/{last_csv_name}.csv'))
    symbols = sorted(df['ticker'].astype(str).unique())
    with open(get_data_path('financeReportDate.csv'), "w") as file:
        file.write('symbol,date\n')
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
                time.sleep(0.3)



if __name__ == '__main__':
    refresh_finance_report_dates()
