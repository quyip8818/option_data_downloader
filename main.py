import datetime

from src.quandl.option_percentiles import fetch_option_percentiles
from src.utils.date_utils import get_last_workday

today = datetime.date(2025, 5, 30)
# today = get_last_workday(datetime.date.today())

SKIP_SYMBOLS = {}

fetch_option_percentiles(today)
# fetch_all_yf_options(today, get_last_iv_rank(), SKIP_SYMBOLS)
