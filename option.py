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
    df['estPrice'] = np.where(mid_price <= 0.01,df['lastPrice'], mid_price)
    df['inMoney']= (current_price - df['strike']).clip(lower=0) if is_call else (df['strike'] - current_price).clip(lower=0)
    df['timeValue'] = df['estPrice'] - df['inMoney']
    return df


def max_time_value(df, current_price):
    sub_df = df[abs(df['strike']-current_price) <= 5]
    return sub_df['timeValue'].max()


def process_max_time_value_df(df, current_price):
    df.sort_values(by="days", inplace=True)
    df['day_value'] = (df['value'] - CONTRACT_COST)/ df['days']
    df['value_percent'] = df['value'] / current_price
    df['day_value_percent'] = df['day_value'] / current_price
    df['year_value_percent'] =  np.where(df['days']  < 7, df['day_value_percent'] * 252, df['day_value_percent'] * 360)


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
        put_df =process_option(opt_chain.puts, current_price, False)

        date = datetime.strptime(date_str, "%Y-%m-%d").date()
        diff_days = (date - today).days + MIN_DAY_DIFF

        call_dfs.append([call_df, f"c_{diff_days}"])
        put_dfs.append([put_df, f"p_{diff_days}"])

        call_max_time_value_df.loc[len(call_max_time_value_df)] = [diff_days, max_time_value(call_df, current_price)]
        put_max_time_value_df.loc[len(put_max_time_value_df)] = [diff_days, max_time_value(put_df, current_price)]

    with pd.ExcelWriter(f"options/{symbol}_{today.strftime("%Y_%m_%d")}.xlsx") as writer:
        process_max_time_value_df(call_max_time_value_df, current_price)
        call_max_time_value_df.to_excel(writer, sheet_name='c_all', index=False)
        process_max_time_value_df(put_max_time_value_df, current_price)
        put_max_time_value_df.to_excel(writer, sheet_name='p_all', index=False)

        for call_df in call_dfs:
            call_df[0].to_excel(writer, sheet_name=call_df[1], index=False)
        for put_df in put_dfs:
            put_df[0].to_excel(writer, sheet_name=put_df[1], index=False)