def get_ba_spread(bid, ask):
    return (ask - bid) / ((ask + bid) / 2)


# Out of money if > 0, In money < 0
def get_otm_ratio(cur_price, strike, is_call):
    if is_call:
        return strike / cur_price - 1
    else:
        return 1 - strike / cur_price
