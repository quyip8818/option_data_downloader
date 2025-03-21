import re
from time import sleep

from src.utils.file_utis import open_file_in_application
from src.utils.path_utils import get_quandl_path


def read_iv_rank(symbol):
    open_file_in_application(get_quandl_path(f"option_iv_rank_by_symbols/{symbol}.csv"))


def get_symbols(str):
    return sorted([w.strip() for w in re.split(r'[\s,]+', str)])

if __name__ == "__main__":
    for symbol in get_symbols("""VRTX,UBER,ABNB"""):
        read_iv_rank(symbol)
        sleep(0.1)
