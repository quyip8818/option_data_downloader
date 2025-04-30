import re
from time import sleep

from src.utils.file_utis import open_file_in_application
from src.utils.path_utils import get_quandl_path, get_latest_date


def read_iv_rank(symbol):
    open_file_in_application(get_quandl_path(f"option_iv_rank_by_symbols/{symbol}.csv"))


def get_symbols(str):
    return sorted([w.strip() for w in re.split(r'[\s,]+', str)])

if __name__ == "__main__":
    latest_date = get_latest_date(get_quandl_path('option_iv_rank'))
    open_file_in_application(get_quandl_path(f"option_iv_rank/{latest_date}.csv"))
    sleep(0.2)
    for symbol in get_symbols("""BSX DASH EOG HES SLNO ZS"""):
        read_iv_rank(symbol)
        sleep(0.2)
