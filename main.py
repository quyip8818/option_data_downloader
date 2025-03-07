import datetime

from src.quandl.option_percentiles import fetch_option_percentiles, get_last_iv_rank

# today = datetime.date(2025, 2, 14)
today = datetime.date.today()

# fetch_option_percentiles(today)

print(get_last_iv_rank())


