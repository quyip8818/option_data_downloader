from time import sleep

import yfinance as yf

def get_stock_info(symbol):
    try:
        print(f'yf stock info: {symbol}')
        sleep(0.4)
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info
        current_price = info["lastPrice"]
        market_cap = info["marketCap"]
        return current_price, market_cap
    except Exception as e:
        if 'Rate limited' in e.args[0]:
            sleep(1)
            raise e
        return None, None


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
