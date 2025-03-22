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


if __name__ == "__main__":
    get_prices(["ADMA", "ALNY", "BZQ", "FENC", "FITE", "FWONA", "MSGE",
     "MUX", "NIC", "QLYS", "RDUS", "RLX", "SKLZ", "SKYU",
     "TARS", "TGTX", "VRNA", "WBD", "XP"])
