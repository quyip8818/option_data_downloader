from src.utils.file_utis import open_file_in_application
from src.utils.path_utils import get_quandl_path


def read_iv_rank(symbol):
    open_file_in_application(get_quandl_path(f"option_iv_rank_by_symbols/{symbol}.csv"))


if __name__ == "__main__":
    for symbol in ['IOT', 'SRPT', 'SU']:
        read_iv_rank(symbol)
