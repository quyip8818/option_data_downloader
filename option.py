import os
from datetime import datetime
import numpy as np
import pandas as pd
import yfinance as yf

CONTRACT_COST = 0.05
MIN_DAY_DIFF = 0


def process_option(df, current_price, is_call):
    df.fillna(0)
    df.drop(columns=["inTheMoney", "contractSize", "currency"], inplace=True)
    df["lastTradeDate"] = df["lastTradeDate"].dt.tz_convert("EST").dt.tz_localize(None)
    mid_price = (df['bid'] + df['ask']) / 2.0
    df['estPrice'] = np.maximum(df['lastPrice'], mid_price)
    df['inMoney'] = (current_price - df['strike']).clip(lower=0) if is_call else (df['strike'] - current_price).clip(
        lower=0)
    df['timeValue'] = df['estPrice'] - df['inMoney']
    df['timeValuePercent'] = df['timeValue'] / current_price
    df['strikePercent'] = df['strike'] / current_price
    return df


def max_time_value(df, current_price):
    df2 = df[['strike', 'timeValue']].copy()
    df2['price_diff'] = abs(df2['strike'] - current_price)
    df2 = df2.sort_values(['price_diff'], ascending=[True]).head(3)
    return df2['timeValue'].max()

def process_max_time_value_df(df, current_price):
    df['dayValue'] = (df['value'] - CONTRACT_COST) / df['days']
    df['valuePercent'] = df['value'] / current_price
    df['dayValuePercent'] = df['dayValue'] / current_price
    df['yearValuePercent'] = np.where(df['days'] < 7, df['dayValuePercent'] * 252, df['dayValuePercent'] * 360)
    df = df[['days', 'valuePercent', 'dayValuePercent', 'yearValuePercent', 'value']]
    return df.sort_values(by="days")


def save_option_data(symbol, today):
    ticker = yf.Ticker(symbol)
    current_price = ticker.fast_info["lastPrice"]

    call_dfs = []
    put_dfs = []
    call_max_time_value_df = pd.DataFrame(columns=["days", "value"])
    put_max_time_value_df = pd.DataFrame(columns=["days", "value"])

    for date_str in ticker.options:
        opt_chain = ticker.option_chain(date_str)

        call_df = process_option(opt_chain.calls, current_price, True)
        put_df = process_option(opt_chain.puts, current_price, False)

        date = datetime.strptime(date_str, "%Y-%m-%d").date()
        diff_days = max((date - today).days, 1)

        call_dfs.append([call_df, f"c_{diff_days}"])
        put_dfs.append([put_df, f"p_{diff_days}"])

        call_max_time_value_df.loc[len(call_max_time_value_df)] = [diff_days, max_time_value(call_df, current_price)]
        put_max_time_value_df.loc[len(put_max_time_value_df)] = [diff_days, max_time_value(put_df, current_price)]

    today_str = today.strftime("%Y_%m_%d")
    os.makedirs(f"options/{today_str}", exist_ok=True)
    with pd.ExcelWriter(f"options/{today_str}/{symbol}_{today_str}.xlsx") as writer:
        current_price_df = pd.DataFrame({
            'rowName': ['currentPrice'],
            'currentPrice': [current_price],
        })
        call_max_time_value_df = process_max_time_value_df(call_max_time_value_df, current_price)
        current_price_df.to_excel(writer, sheet_name='c_all', index=False, header=False, startrow=0)
        call_max_time_value_df.to_excel(writer, sheet_name='c_all', index=False, startrow=2)

        put_max_time_value_df = process_max_time_value_df(put_max_time_value_df, current_price)
        current_price_df.to_excel(writer, sheet_name='p_all', index=False, header=False, startrow=0)
        put_max_time_value_df.to_excel(writer, sheet_name='p_all', index=False, startrow=2)

        for call_df in call_dfs:
            current_price_df.to_excel(writer, sheet_name=call_df[1], index=False, header=False, startrow=0)
            call_df[0].to_excel(writer, sheet_name=call_df[1], index=False, startrow=2)
        for put_df in put_dfs:
            current_price_df.to_excel(writer, sheet_name=put_df[1], index=False, header=False, startrow=0)
            put_df[0].to_excel(writer, sheet_name=put_df[1], index=False, startrow=2)
