import math

import numpy as np
import pandas as pd


# get the index of the value, or idx just next to it
# series should already sorted small to large
def get_exact_or_next_idx(series, value):
    idx = np.searchsorted(series, value, side="left")
    return idx.item()

# get the index of the value, or idx just prev to it
# get the index of value, or just larger


def get_exact_or_prev_idx(series, value):
    idx = np.searchsorted(series, value, side="right")
    return idx.item() - 1


def get_percentile(percentiles: pd.Series, value):
    idx = get_exact_or_next_idx(percentiles.values, value)
    return percentiles.index[idx] if idx < len(percentiles) else 1


def get_percentile_rank(percentiles, value):
    if not isinstance(value, float) or math.isnan(value):
        return None
    return math.floor(get_percentile(percentiles, value) * 100)


def get_exact_or_prev_value(values: pd.Series, date: pd.DatetimeIndex):
    idx = get_exact_or_prev_idx(values.index, date)
    if idx < 0:
        return None
    day_diff = (values.index[idx] - date).days
    if abs(day_diff) >= 3:
        return None
    return values.iloc[idx]
