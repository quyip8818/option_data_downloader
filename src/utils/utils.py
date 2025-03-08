import math


def round_num(num, point):
    if isinstance(num, float):
        return round(num, point)
    return num


def get_percentile(value, percentiles):
    for percentile, v in percentiles.items():
        if v > value:
            return percentile
    return 1

def get_percentile_rank(value, percentiles):
    return math.floor(get_percentile(value, percentiles) * 100)
