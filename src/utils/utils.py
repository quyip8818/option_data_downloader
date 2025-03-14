import os


def get_symbols_from_folders(folder):
    symbols = [file_name.replace('.csv', '') for file_name in os.listdir(
        folder) if file_name.endswith('.csv')]
    return sorted([s for s in symbols if len(s) <= 5])


def round_num(num, point):
    if isinstance(num, float):
        return round(num, point)
    return num
