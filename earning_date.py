import pandas as pd
import yfinance as yf


def get_earning_data(symbol):
    ticker = yf.Ticker(symbol)
    df = ticker.get_earnings_dates(limit=100)
    df = df[df['Reported EPS'].notna()]
    date= df.index.tz_convert('US/Eastern').date
    date_series = pd.Series(date, name='date')
    df = pd.DataFrame(date_series)
    df['symbol'] = symbol
    df = df[['symbol', 'date']]
    df.to_csv(f'finance/{symbol}.csv', index=False)


df = pd.read_csv('raw/symbols.csv')
for symbol in df['symbols'].unique():
    try:
        get_earning_data(symbol)
        print(symbol)
    except Exception as e:
        print(e)

