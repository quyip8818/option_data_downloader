from datetime import datetime
import numpy as np
import pandas as pd
import pytz
import yfinance as yf

from src.utils.option_utils import get_ba_spread
from src.utils.utils import round_num

MIN_TIME_VALUE = 0.05
CONTRACT_COST = 0.05
MIN_DAY_DIFF = 0
LAST_TRADE_DATE_FORMAT = '%m/%d/%Y %I:%M:%S'

def get_est_price(ask, bid, last, last_trade_date, today):
    if not isinstance(last_trade_date, pd.Timestamp):
        return (ask + bid) / 2

    last_trade_date = last_trade_date.to_pydatetime()
    diff_days = abs((today - last_trade_date.date()).days)
    valid_last_day = diff_days == 0 if today.weekday() <= 5 else diff_days <= 2

    mid = (ask + bid) / 2
    if valid_last_day and ask >= last >= bid:
        return (mid + last) / 2
    else:
        return mid

def process_option(df_raw, current_price, is_call, today):
    df2 = df_raw.copy()
    df2.drop(columns=["inTheMoney", "contractSize", "currency"], inplace=True)
    df2 = df2.dropna(subset=['contractSymbol', 'lastTradeDate', 'strike', 'lastPrice', 'bid', 'ask'])
    df2.fillna(0)
    df2["lastTradeDate"] = df2["lastTradeDate"].dt.tz_convert("EST").dt.tz_localize(None)

    df = df2.copy()
    df = df[(df['bid'] > 0) & (df['ask'] > 0) & (df['lastPrice'] > 0)]
    df['estPrice'] = df.apply(lambda row: get_est_price(row['ask'], row['bid'], row['lastPrice'], row['lastTradeDate'], today), axis=1)
    df['ba_spread'] = df.apply(lambda row: get_ba_spread(row['bid'], row['ask']), axis=1)

    df['inMoney'] = (current_price - df['strike']).clip(lower=0) if is_call else (df['strike'] - current_price).clip(
        lower=0)
    df['timeValue'] = df['estPrice'] - df['inMoney']
    df['timeValuePercent'] = df['timeValue'] / current_price
    df['strikePercent'] = abs(df['strike'] - current_price) / current_price

    df = df[df['timeValue'] > MIN_TIME_VALUE]

    df2_filtered = df2[~df2['contractSymbol'].isin(df['contractSymbol'])]
    df = pd.concat([df, df2_filtered], ignore_index=True).sort_values(by="strike").reset_index(drop=True)

    return df[
        ['contractSymbol', 'strike', 'estPrice', 'timeValue', 'timeValuePercent', 'strikePercent', 'ba_spread', 'volume', 'openInterest',
         'inMoney', 'impliedVolatility', 'lastTradeDate', 'lastPrice', 'bid', 'ask', 'change', 'percentChange']]


def get_max_time_value(df, current_price):
    df = df.dropna(subset=["estPrice"])
    if df.empty:
        return np.nan, np.nan, np.nan, np.nan, np.nan
    df2 = df[['strike', 'timeValue', 'impliedVolatility', 'volume', 'openInterest', 'bid', 'ask', 'ba_spread']].copy()
    df2['price_diff'] = abs(df2['strike'] - current_price)
    top_row = df2.sort_values(['price_diff'], ascending=[True]).iloc[0]
    return (float(top_row['timeValue']),
            float(top_row['impliedVolatility']),
            float(top_row['volume']),
            float(top_row['openInterest']),
            float(top_row['ba_spread']),)


MAX_TIME_VALUE_DAY_OFFSET_ORDER = [7, 6, 5, 4, 8, 9, 10, 11, 12, 13, 14, 15]
def process_max_time_value_df(df_raw, current_price):
    df = df_raw.copy()
    df['dayValue'] = (df['value'] - CONTRACT_COST) / df['days']
    df['valuePercent'] = df['value'] / current_price
    df['dayValuePercent'] = df['dayValue'] / current_price
    df['yearValuePercent'] = np.where(df['days'] < 7, df['dayValuePercent'] * 252, df['dayValuePercent'] * 360)

    df['valPct'] = df['valuePercent'].apply(lambda x: f"{x:.2%}")
    df['dayValPct'] = df['dayValuePercent'].apply(lambda x: f"{x:.2%}")
    df['yearValPct'] = df['yearValuePercent'].apply(lambda x: f"{x:.2%}")
    df['val'] = df['value'].apply(lambda x: f"{x:.3f}")
    df['IV'] = df['impliedVolatility'].apply(lambda x: f"{x:.3f}")
    df[' '] = ''
    df = df[
        ['days', 'valPct', 'dayValPct', 'yearValPct', 'val', 'IV', 'volume', 'openInterest', 'ba_spread', ' ',
         'valuePercent', 'dayValuePercent', 'yearValuePercent', 'value', 'impliedVolatility']]

    week_row = None
    for day_offset in MAX_TIME_VALUE_DAY_OFFSET_ORDER:
        if (df['days'] == day_offset).any():
            week_row = df[df['days'] == day_offset].iloc[0]
            break
    if week_row is None:
        return df.sort_values(by="days"), ['', '', ''], ['', '', ''], ['', '', ''], ['', '', ''], ['', '', '']

    max_day_row = df.loc[df['days'].idxmax()]
    payback_weeks = max_day_row['value'] / week_row['value'] \
        if max_day_row['value'].any() and week_row['value'].any() else ''

    long_term_min_iv = df[df['days'] > 100]['impliedVolatility'].min()


    iv_ratio = long_term_min_iv / week_row['impliedVolatility'] \
        if long_term_min_iv.any() and week_row['impliedVolatility'].any() else ''

    volume_ratio = max_day_row['volume'] / week_row['volume'] \
        if max_day_row['volume'].any() and week_row['volume'].any() else ''

    open_interest_ratio = max_day_row['openInterest'] / week_row['openInterest'] \
        if max_day_row['openInterest'].any() and week_row['openInterest'].any() else ''

    return (df.sort_values(by="days"),
            [round_num(payback_weeks, 2), week_row['valPct'], max_day_row['valPct']],
            [round_num(iv_ratio, 3), round_num(week_row['impliedVolatility'], 3), round_num(long_term_min_iv, 3), round_num(max_day_row['impliedVolatility'], 3)],
            [round_num(volume_ratio, 2), week_row['volume'], max_day_row['volume']],
            [round_num(open_interest_ratio, 2), week_row['openInterest'], max_day_row['openInterest']],
            ['', week_row['ba_spread'], max_day_row['ba_spread']],
        )


def process_header_data(writer, sheet_name, data):
    pd.DataFrame.from_dict(data, orient="index").to_excel(writer, sheet_name=sheet_name, index=True, header=False, startrow=0)
    return len(data) + 1


def process_option_data(symbol, folder, file_name, today):
    today_str = today.strftime("%Y_%m_%d")
    ticker = yf.Ticker(symbol)
    try:
        current_price = ticker.fast_info["lastPrice"]
    except Exception as e:
        if 'Rate limited' in e.args[0]:
            raise e
        return None

    earnings_dates = ticker.earnings_dates
    if earnings_dates is None or earnings_dates.empty:
        next_earnings_date = pd.NaT
    else:
        today_pd = pd.Timestamp(today, tz=pytz.timezone("America/New_York"))
        try:
            next_earnings_date = earnings_dates[earnings_dates.index > today_pd].index.min().to_pydatetime()
        except:
            next_earnings_date = pd.NaT

    call_dfs = []
    put_dfs = []
    call_max_time_value_df = pd.DataFrame(columns=["days", "value", "impliedVolatility", 'volume', 'openInterest', 'ba_spread'])
    put_max_time_value_df = pd.DataFrame(columns=["days", "value", "impliedVolatility", 'volume', 'openInterest', 'ba_spread'])

    for date_str in ticker.options:
        opt_chain = ticker.option_chain(date_str)

        call_df = process_option(opt_chain.calls, current_price, True, today)
        put_df = process_option(opt_chain.puts, current_price, False, today)

        date = datetime.strptime(date_str, "%Y-%m-%d").date()
        diff_days = max((date - today).days, 1)

        call_dfs.append([call_df, f"c_{diff_days}"])
        put_dfs.append([put_df, f"p_{diff_days}"])

        call_max_time_value = get_max_time_value(call_df, current_price)
        call_max_time_value_df.loc[len(call_max_time_value_df)] = [diff_days, *call_max_time_value]

        put_max_time_value = get_max_time_value(put_df, current_price)
        put_max_time_value_df.loc[len(put_max_time_value_df)] = [diff_days, *put_max_time_value]

    call_max_time_value_df, call_paybacks, call_ivs, call_volumes, call_open_interest, call_ba_spread = process_max_time_value_df(call_max_time_value_df, current_price)
    put_max_time_value_df, put_paybacks, put_ivs, put_volumes, put_open_interest, put_ba_spread = process_max_time_value_df(put_max_time_value_df, current_price)

    with pd.ExcelWriter(f"{folder}/{file_name}.xlsx") as writer:
        next_start_row = process_header_data(writer, "c_all", {
            "today": [today_str],
            "currentPrice": [current_price],
            "paybacks": call_paybacks,
            "ivs": call_ivs,
            "volumes": call_volumes,
            "open_interest": call_open_interest,
            "ba_spread": call_ba_spread,
        })
        call_max_time_value_df.to_excel(writer, sheet_name='c_all', index=False, startrow=next_start_row)

        next_start_row = process_header_data(writer, "p_all", {
            "today": [today_str],
            "currentPrice": [current_price],
            "paybacks": put_paybacks,
            "ivs": put_ivs,
            "volumes": put_volumes,
            "open_interest": put_open_interest,
            "ba_spread": put_ba_spread,
        })
        put_max_time_value_df.to_excel(writer, sheet_name='p_all', index=False, startrow=next_start_row)

        current_price_df = pd.DataFrame({
            'rowName': ['currentPrice'],
            'currentPrice': [current_price],
        })
        for call_df in call_dfs:
            current_price_df.to_excel(writer, sheet_name=call_df[1], index=False, header=False, startrow=0)
            call_df[0].to_excel(writer, sheet_name=call_df[1], index=False, startrow=2)
        for put_df in put_dfs:
            current_price_df.to_excel(writer, sheet_name=put_df[1], index=False, header=False, startrow=0)
            put_df[0].to_excel(writer, sheet_name=put_df[1], index=False, startrow=2)

    return [next_earnings_date, current_price, call_paybacks, call_ivs, call_volumes, call_open_interest, call_ba_spread, put_paybacks, put_ivs, put_volumes, put_open_interest, put_ba_spread]


def has_option(symbol):
    ticker = yf.Ticker(symbol)
    return len(ticker.options) >= 10
