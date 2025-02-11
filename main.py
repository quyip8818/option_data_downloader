import csv
import datetime
import os
import pandas as pd
import time

from option import process_option_data
from symbols import Symbols

# today = datetime.date(2025, 2, 7)
today = datetime.date.today()

today_str = today.strftime("%Y_%m_%d")
folder = f"options/{today_str}"
os.makedirs(folder, exist_ok=True)

file_name = f"{folder}/AAA_summary.csv"
is_first_run = not os.path.exists(file_name)

with open(file_name, "a", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    if is_first_run:
        writer.writerow(
            ['symbol',
             'c_pb_w', 'c_week_valPct', 'c_max_valPct', 'c_iv_r', 'c_week_iv', 'c_max_iv',
             'c_vol_r', 'c_week_vol', 'c_max_vol', 'c_oi_r', 'c_week_oi', 'c_max_oi', 'c_week_ba_diff', 'c_max_ba_diff',
             '|', 'p_pb_w', 'p_week_valPct', 'p_max_valPct', 'p_iv_r', 'p_week_iv', 'p_max_iv',
             'p_vol_r', 'p_week_vol', 'p_max_vol', 'p_oi_r', 'p_week_oi', 'p_max_oi', 'p_week_ba_diff', 'p_max_ba_diff',])
        symbols_set = set()
    else:
        df = pd.read_csv(file_name)
        symbols_set = set(df['symbol'])

    for idx, symbol in enumerate(Symbols):
        if symbol in symbols_set:
            continue
        print(f'processing {idx}: {symbol}')
        [call_paybacks, call_ivs, call_volumes, call_open_interest, call_bid_ask_diff, put_paybacks, put_ivs, put_volumes,
         put_open_interest, put_bid_ask_diff] = process_option_data(symbol, folder, f"{symbol}_{today_str}", today)

        summary_row = [symbol, *call_paybacks, *call_ivs, *call_volumes, *call_open_interest, call_bid_ask_diff[1], call_bid_ask_diff[2],
                       '|',*put_paybacks, *put_ivs, *put_volumes, *put_open_interest, put_bid_ask_diff[1], put_bid_ask_diff[2]]
        writer.writerow(summary_row)
        csvfile.flush()
        time.sleep(2)
