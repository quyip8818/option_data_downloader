import datetime
from option import process_option_data

# today = datetime.date(2025, 2, 7)
today = datetime.date.today()

process_option_data('LQD', 'test', 'LQD', today)