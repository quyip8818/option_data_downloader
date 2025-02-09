import datetime
from option import save_option_data

today = datetime.date(2025, 2, 7)
# today = datetime.date.today()

save_option_data('TLT', today)
save_option_data('GLD', today)
save_option_data('IAU', today)
save_option_data('SLV', today)
save_option_data('TMO', today)
