import datetime

from src.quandl.option_percentiles import get_last_iv_rank, fetch_option_percentiles
from src.utils.date_utils import get_last_workday
from src.yfinance.all_options import fetch_all_yf_options

# today = datetime.date(2025, 3, 6)
today = get_last_workday(datetime.date.today())

SKIP_SYMBOLS = {}

fetch_option_percentiles(today)
iv_rank_df = get_last_iv_rank()
fetch_all_yf_options(today, iv_rank_df, SKIP_SYMBOLS)

