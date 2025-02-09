import os
from datetime import datetime
import numpy as np
import pandas as pd
import yfinance as yf

CONTRACT_COST = 0.05
MIN_DAY_DIFF = 0


def get_est_price(ask, bid, last):
    mid = (ask + bid) / 2
    return mid if (last > mid or last < mid) else (mid + last) / 2


def process_option(df_raw, current_price, is_call):
    df = df_raw.copy()
    df.fillna(0)
    df.drop(columns=["inTheMoney", "contractSize", "currency"], inplace=True)
    df["lastTradeDate"] = df["lastTradeDate"].dt.tz_convert("EST").dt.tz_localize(None)

    df = df[(df['bid'] > 0) & (df['ask'] > 0)]
    df['estPrice'] = df.apply(lambda row: get_est_price(row['ask'], row['bid'], row['lastPrice']), axis=1)

    df['inMoney'] = (current_price - df['strike']).clip(lower=0) if is_call else (df['strike'] - current_price).clip(
        lower=0)
    df['timeValue'] = df['estPrice'] - df['inMoney']
    df['timeValuePercent'] = df['timeValue'] / current_price
    df['strikePercent'] = df['strike'] / current_price
    return df


def max_time_value(df, current_price):
    df2 = df[['strike', 'timeValue', 'impliedVolatility']].copy()
    df2['price_diff'] = abs(df2['strike'] - current_price)
    df2 = df2.sort_values(['price_diff'], ascending=[True]).head(3)
    df2 = df2.sort_values(['timeValue'], ascending=[False]).head(1)
    return float(df2['timeValue'].item()), float(df2['impliedVolatility'].item())


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
    df['x'] = ''
    df = df[
        ['days', 'valPct', 'dayValPct', 'yearValPct', 'val', 'IV', 'x', 'valuePercent', 'dayValuePercent',
         'yearValuePercent', 'value', 'impliedVolatility']]

    if (df['days'] == 7).any():
        week_row = df[df['days'] == 7].iloc[0]
    elif (df['days'] == 6).any():
        week_row = df[df['days'] == 6].iloc[0]
    elif (df['days'] == 5).any():
        week_row = df[df['days'] == 5].iloc[0]
    else:
        week_row = df[df['days'] == 4].iloc[0]

    max_day_row = df.loc[df['days'].idxmax()]
    payback_weeks = max_day_row['value'] / week_row['value'] if max_day_row['value'].any() and week_row[
        'value'].any() else ''

    return df.sort_values(by="days"), [payback_weeks, week_row['valPct'], max_day_row['valPct']]


def save_header_data(writer, sheet_name, data):
    pd.DataFrame.from_dict(data, orient="index").to_excel(writer, sheet_name=sheet_name, index=True, header=False,
                                                          startrow=0)
    return len(data) + 1


def save_option_data(symbol, today):
    ticker = yf.Ticker(symbol)
    current_price = ticker.fast_info["lastPrice"]

    call_dfs = []
    put_dfs = []
    call_max_time_value_df = pd.DataFrame(columns=["days", "value", "impliedVolatility"])
    put_max_time_value_df = pd.DataFrame(columns=["days", "value", "impliedVolatility"])

    for date_str in ticker.options:
        opt_chain = ticker.option_chain(date_str)

        call_df = process_option(opt_chain.calls, current_price, True)
        put_df = process_option(opt_chain.puts, current_price, False)

        date = datetime.strptime(date_str, "%Y-%m-%d").date()
        diff_days = max((date - today).days, 1)

        call_dfs.append([call_df, f"c_{diff_days}"])
        put_dfs.append([put_df, f"p_{diff_days}"])

        call_max_time_value_df.loc[len(call_max_time_value_df)] = [diff_days, *max_time_value(call_df, current_price)]
        put_max_time_value_df.loc[len(put_max_time_value_df)] = [diff_days, *max_time_value(put_df, current_price)]

    today_str = today.strftime("%Y_%m_%d")
    os.makedirs(f"options/{today_str}", exist_ok=True)

    with pd.ExcelWriter(f"options/{today_str}/{symbol}_{today_str}.xlsx") as writer:
        call_max_time_value_df, paybacks = process_max_time_value_df(call_max_time_value_df, current_price)
        next_start_row = save_header_data(writer, "c_all", {
            "currentPrice": [current_price],
            "paybacks": paybacks
        })
        call_max_time_value_df.to_excel(writer, sheet_name='c_all', index=False, startrow=next_start_row)

        put_max_time_value_df, paybacks = process_max_time_value_df(put_max_time_value_df, current_price)
        next_start_row = save_header_data(writer, "p_all", {
            "currentPrice": [current_price],
            "paybacks": paybacks
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
