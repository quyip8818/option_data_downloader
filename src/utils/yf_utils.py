import yfinance as yf

def get_prices(symbols):
    tickers = yf.Ticker(" ".join(symbols))
    for symbol in symbols:
        try:
            print(tickers.ticker)
            # price = tickers.ticker[symbol].fast_info['last_price']
            # print(f"{symbol}: ${price:.2f}")
        except KeyError:
            print(f"{symbol}: Price not available")


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
