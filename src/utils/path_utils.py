import os
from pathlib import Path

# Find the project root by looking for a known file or folder (like .git or pyproject.toml)
root_dir = Path(__file__).resolve().parents[
    next(i for i, p in enumerate(Path(__file__).resolve().parents) if (p / ".git").exists() or (p / "pyproject.toml").exists())
]


def get_quandl_option_iv_raw_path(date):
    return f'{root_dir}/quandl/option_iv_raw/{date}.csv'



def get_quandl_option_iv_rank_path(date):
    return f'{root_dir}/quandl/option_iv_rank/{date}.csv'


def get_quandl_option_iv_rank_latest():
    folder = f'{root_dir}/quandl/option_iv_rank'
    dates = [f.lower().replace('.csv', '') for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
    dates = [d for d in dates if len(d) == 10]
    last_date = sorted(dates)[-1]
    return get_quandl_option_iv_rank_path(last_date)


def get_quandl_option_iv_percentiles_path(header):
    return f'{root_dir}/quandl/option_iv_percentiles/{header}.csv'


def get_raw_path(file):
    return f'{root_dir}/raw/{file}'


def get_src_module_path(path):
    return f'{root_dir}/src/{path}'