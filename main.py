import datetime

from src.quandl.option_percentiles import fetch_option_percentiles


# today = datetime.date(2025, 2, 14)
today = datetime.date.today()

fetch_option_percentiles(today)


