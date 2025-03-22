from time import sleep

import yfinance as yf

def get_current_price(symbol):
    try:
        print(f'get_price {symbol}')
        ticker = yf.Ticker(symbol)
        current_price = ticker.fast_info["lastPrice"]
        sleep(0.2)
        return current_price
    except Exception as e:
        if 'Rate limited' in e.args[0]:
            sleep(1)
            raise e
        return None


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


if __name__ == "__main__":
    get_prices(["ADMA", "ALNY", "BZQ", "FENC", "FITE", "FWONA", "MSGE",
     "MUX", "NIC", "QLYS", "RDUS", "RLX", "SKLZ", "SKYU",
     "TARS", "TGTX", "VRNA", "WBD", "XP"])
