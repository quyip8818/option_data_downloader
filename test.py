import datetime
from src.yfinance.option import process_option_data

# today = datetime.date(2025, 2, 7)
today = datetime.date.today()

processed_data = process_option_data('IGV', 'test', 'IGV', today)
if processed_data is not None:
    [next_earnings_date, current_price, call_paybacks, call_ivs, call_volumes, call_open_interest,
     call_bid_ask_diff, put_paybacks, put_ivs, put_volumes, put_open_interest, put_bid_ask_diff] = processed_data
