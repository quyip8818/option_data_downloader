import dask.dataframe as dd
import csv

import pandas as pd
from dask.diagnostics import ProgressBar

from src.utils.path_utils import get_quandl_path


def get_all_symbols():
    etf_symbols = []
    with open("../../option_trade/raw/symbols.csv", "r") as file:
        reader = csv.reader(file)
        for row in reader:
            etf_symbols.append(row[0])
    return etf_symbols

# etf_symbols = set()
# with open("symbols.csv", "r") as file:
#     reader = csv.reader(file)
#     for row in reader:
#         etf_symbols.add(row[0])
# etf_symbols = list(etf_symbols)
# etf_symbols.sort()
#
# with open("symbols2.csv", "w", newline="") as file:
#     writer = csv.writer(file)
#     for etf in etf_symbols:
#         writer.writerow([etf])


if __name__ == "__main__":
    with ProgressBar():
        df = dd.read_csv(get_quandl_path(f'option_iv_raw/*.csv'))
        symbols = df['ticker'].unique().compute()
        symbols = sorted(symbols.dropna())
        df = pd.DataFrame(symbols, columns=["symbol"])
        df.to_csv("symbols.csv", index=False)

