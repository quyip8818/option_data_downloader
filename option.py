from datetime import datetime
import numpy as np
import pandas as pd
import yfinance as yf

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
    df = df_raw.copy()
    df.drop(columns=["inTheMoney", "contractSize", "currency"], inplace=True)
    df = df.dropna(subset=['contractSymbol', 'lastTradeDate', 'strike', 'lastPrice', 'bid', 'ask'])
    df.fillna(0)
    df["lastTradeDate"] = df["lastTradeDate"].dt.tz_convert("EST").dt.tz_localize(None)

    df = df[(df['bid'] > 0) & (df['ask'] > 0) & (df['lastPrice'] > 0)]
    df['estPrice'] = df.apply(lambda row: get_est_price(row['ask'], row['bid'], row['lastPrice'], row['lastTradeDate'], today), axis=1)

    df['inMoney'] = (current_price - df['strike']).clip(lower=0) if is_call else (df['strike'] - current_price).clip(
        lower=0)
    df['timeValue'] = df['estPrice'] - df['inMoney']
    df['timeValuePercent'] = df['timeValue'] / current_price
    df['strikePercent'] = df['strike'] / current_price
    return df[df['timeValue'] > MIN_TIME_VALUE]


def get_max_time_value(df, current_price):
    if df.empty:
        return np.nan, np.nan, np.nan, np.nan, np.nan
    df2 = df[['strike', 'timeValue', 'impliedVolatility', 'volume', 'openInterest', 'bid', 'ask']].copy()
    df2['price_diff'] = abs(df2['strike'] - current_price)
    top_row = df2.sort_values(['price_diff'], ascending=[True]).iloc[0]
    return (float(top_row['timeValue']),
            float(top_row['impliedVolatility']),
            float(top_row['volume']),
            float(top_row['openInterest']),
            round(float(top_row['ask']) - float(top_row['bid']), 2))


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
        ['days', 'valPct', 'dayValPct', 'yearValPct', 'val', 'IV', 'volume', 'openInterest', 'bidAskDiff', ' ',
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

    iv_ratio = max_day_row['impliedVolatility'] / week_row['impliedVolatility'] \
        if max_day_row['impliedVolatility'].any() and week_row['impliedVolatility'].any() else ''

    volume_ratio = max_day_row['volume'] / week_row['volume'] \
        if max_day_row['volume'].any() and week_row['volume'].any() else ''

    open_interest_ratio = max_day_row['openInterest'] / week_row['openInterest'] \
        if max_day_row['openInterest'].any() and week_row['openInterest'].any() else ''

    return (df.sort_values(by="days"),
            [payback_weeks, week_row['valPct'], max_day_row['valPct']],
            [iv_ratio, week_row['impliedVolatility'], max_day_row['impliedVolatility']],
            [volume_ratio, week_row['volume'], max_day_row['volume']],
            [open_interest_ratio, week_row['openInterest'], max_day_row['openInterest']],
            ['', week_row['bidAskDiff'], max_day_row['bidAskDiff']],
        )


def process_header_data(writer, sheet_name, data):
    pd.DataFrame.from_dict(data, orient="index").to_excel(writer, sheet_name=sheet_name, index=True, header=False, startrow=0)
    return len(data) + 1


def process_option_data(symbol, folder, file_name, today):
    ticker = yf.Ticker(symbol)
    current_price = ticker.fast_info["lastPrice"]

    call_dfs = []
    put_dfs = []
    call_max_time_value_df = pd.DataFrame(columns=["days", "value", "impliedVolatility", 'volume', 'openInterest', 'bidAskDiff'])
    put_max_time_value_df = pd.DataFrame(columns=["days", "value", "impliedVolatility", 'volume', 'openInterest', 'bidAskDiff'])

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

    call_max_time_value_df, call_paybacks, call_ivs, call_volumes, call_open_interest, call_bid_ask_diff = process_max_time_value_df(call_max_time_value_df, current_price)
    put_max_time_value_df, put_paybacks, put_ivs, put_volumes, put_open_interest, put_bid_ask_diff = process_max_time_value_df(put_max_time_value_df, current_price)

    with pd.ExcelWriter(f"{folder}/{file_name}.xlsx") as writer:
        next_start_row = process_header_data(writer, "c_all", {
            "currentPrice": [current_price],
            "paybacks": call_paybacks,
            "ivs": call_ivs,
            "volumes": call_volumes,
            "open_interest": call_open_interest,
            "bid_ask_diff": call_bid_ask_diff,
        })
        call_max_time_value_df.to_excel(writer, sheet_name='c_all', index=False, startrow=next_start_row)

        next_start_row = process_header_data(writer, "p_all", {
            "currentPrice": [current_price],
            "paybacks": put_paybacks,
            "ivs": put_ivs,
            "volumes": put_volumes,
            "open_interest": put_open_interest,
            "bid_ask_diff": put_bid_ask_diff,
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

    return [current_price, call_paybacks, call_ivs, call_volumes, call_open_interest, call_bid_ask_diff, put_paybacks, put_ivs, put_volumes, put_open_interest, put_bid_ask_diff ]