import datetime
from src.yfinance.option import process_option_data

# today = datetime.date(2025, 2, 7)
today = datetime.date.today()

process_option_data('TTD', 'test', 'TTD', today)