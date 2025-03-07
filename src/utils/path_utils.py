from pathlib import Path

# Find the project root by looking for a known file or folder (like .git or pyproject.toml)
root_dir = Path(__file__).resolve().parents[
    next(i for i, p in enumerate(Path(__file__).resolve().parents) if (p / ".git").exists() or (p / "pyproject.toml").exists())
]


def get_quandl_option_iv_raw_path(date):
    return f'{root_dir}/quandl/option_iv_raw/{date}.csv'



def get_quandl_option_iv_rank_path(date):
    return f'{root_dir}/quandl/option_iv_rank/{date}.csv'



def get_quandl_option_iv_percentiles_path(header):
    return f'{root_dir}/quandl/option_iv_percentiles/{header}.csv'


def get_raw_path(file):
    return f'{root_dir}/raw/{file}'


def get_src_module_path(path):
    return f'{root_dir}/src/{path}'