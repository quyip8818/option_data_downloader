import os
from pathlib import Path

# Find the project root by looking for a known file or folder (like .git or pyproject.toml)
root_dir = (
    Path(__file__)
    .resolve()
    .parents[
        next(
            i
            for i, p in enumerate(Path(__file__).resolve().parents)
            if (p / ".git").exists() or (p / "pyproject.toml").exists()
        )
    ]
)


def get_root_path(path):
    return f"{root_dir}/{path}"


def get_quandl_path(folder):
    return f"{root_dir}/quandl/{folder}"


def get_data_path(folder):
    return f"{root_dir}/../option_data/{folder}"


def get_raw_path(file):
    return f"{root_dir}/raw/{file}"


def get_src_module_path(path):
    return f"{root_dir}/src/{path}"


def extract_file_name(path):
    return os.path.splitext(os.path.basename(path))[0]


def list_csv_file_names(folder):
    files = [
        f.replace(".csv", "")
        for f in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, f)) and f.endswith(".csv")
    ]
    return sorted(files)


def get_latest_date(folder):
    dates = list_csv_file_names(folder)
    dates = [d for d in dates if len(d) == 10]
    last_date = sorted(dates)[-1]
    return last_date
