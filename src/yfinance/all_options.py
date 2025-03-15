import csv
import os
import pandas as pd
import time

from src.yfinance.option import process_option_data
from src.symbols import symbols

def decode_iv_rank(iv_rank_df, symbol):
    if symbol not in iv_rank_df.index:
        return (None, None, None), (None, None, None)
    iv_rank = iv_rank_df.loc[symbol]
    return ((iv_rank['ivcall10'], iv_rank['ivcall10_rank'], iv_rank['ivmean10'], iv_rank['ivmean10_rank'], iv_rank['ivcall1080'], iv_rank['ivcall1080_rank']),
            (iv_rank['ivput10'], iv_rank['ivput10_rank'], iv_rank['ivmean10'], iv_rank['ivmean180_rank'], iv_rank['ivput1080'], iv_rank['ivput1080_rank']))


def fetch_all_yf_options(today, iv_rank_df, skip_symbols):
    today_str = today.strftime("%Y_%m_%d")
    folder = f"options/{today_str}"
    os.makedirs(folder, exist_ok=True)

    file_name = f"{folder}/A_summary_{today_str}.csv"
    file_error_name = f"{folder}/error.csv"
    is_first_run = not os.path.exists(file_name)

    with open(file_name, "a", newline="", encoding="utf-8") as csvfile, open(file_error_name, "a", newline="", encoding="utf-8") as errorfile:
        writer = csv.writer(csvfile)
        error_writer = csv.writer(errorfile)
        if is_first_run:
            writer.writerow(
                ['symbol', 'next_earnings_days', 'price',
                 'c_pb_w', 'c_week_valPct', 'c_max_valPct',
                 'c_iv_10', 'c_iv_10_rank', 'm_iv_10', 'm_iv_10_rank', 'c_iv_1080', 'c_iv_1080_rank',
                 'c_iv_r', 'c_week_iv', 'c_yearly_min_iv', 'c_max_iv',
                 'c_vol_r', 'c_week_vol', 'c_max_vol', 'c_oi_r', 'c_week_oi', 'c_max_oi', 'c_week_ba_spread', 'c_max_ba_spread',
                 '|', 'p_pb_w', 'p_week_valPct', 'p_max_valPct',
                 'p_iv_10', 'p_iv_10_rank', 'm_iv_10', 'm_iv_10_rank', 'p_iv_1080', 'p_iv_1080_rank',
                 'p_iv_r', 'p_week_iv',  'p_yearly_min_iv', 'p_max_iv',
                 'p_vol_r', 'p_week_vol', 'p_max_vol', 'p_oi_r', 'p_week_oi', 'p_max_oi', 'p_week_ba_spread', 'p_max_ba_spread',])
            symbols_set = set()
        else:
            df = pd.read_csv(file_name)
            symbols_set = set(df['symbol'])

        for idx, symbol in enumerate(symbols):
            if symbol in symbols_set or symbol in skip_symbols:
                continue
            print(f'processing {idx}: {symbol}')
            processed_data = process_option_data(symbol, folder, f"{symbol}_{today_str}", today)
            if processed_data is None:
                error_writer.writerow([symbol])
                errorfile.flush()
                continue

            [next_earnings_date, current_price, call_paybacks, call_ivs, call_volumes, call_open_interest,
             call_bid_ask_diff, put_paybacks, put_ivs, put_volumes, put_open_interest, put_bid_ask_diff] = processed_data

            next_earnings_days = '' if pd.isna(next_earnings_date) else (next_earnings_date.date() - today).days

            [call_iv_rank, put_iv_rank] = decode_iv_rank(iv_rank_df, symbol)

            summary_row = [symbol, next_earnings_days, current_price, *call_paybacks, *call_iv_rank, *call_ivs, *call_volumes, *call_open_interest, call_bid_ask_diff[1], call_bid_ask_diff[2],
                           '|',*put_paybacks, *put_iv_rank, *put_ivs, *put_volumes, *put_open_interest, put_bid_ask_diff[1], put_bid_ask_diff[2]]

            try:
                if float(call_ivs[0]) <= 0.7 and float(put_ivs[0]) <= 0.7 and \
                        float(call_ivs[2]) <= 0.3 and float(put_ivs[2]) <= 0.3 and \
                        float(call_bid_ask_diff[1]) <= 0.6 and float(put_bid_ask_diff[1]) <= 0.6:
                    print(summary_row)
            except:
                pass

            writer.writerow(summary_row)
            csvfile.flush()
            time.sleep(2)


# with open(f"summary.csv", "a", newline="", encoding="utf-8") as csvfile:
#     writer = csv.writer(csvfile)
#     for symbol in Symbols:
#         print(f'processing: {symbol}')
#         if has_option(symbol):
#             writer.writerow([symbol])
#             csvfile.flush()
#             time.sleep(0.3)


