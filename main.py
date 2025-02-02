import datetime
from option import save_option_data

today = datetime.date(2025, 1, 31)
# today = datetime.date.today()

save_option_data('TLT', today)
save_option_data('GLD', today)

