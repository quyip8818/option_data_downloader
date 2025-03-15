from datetime import timedelta

def get_last_workday(date):
    if date.weekday() == 5: # Saturday
        recent = date - timedelta(days=1)
    elif date.weekday() == 6:  # Sunday
        recent = date - timedelta(days=2)
    else:
        recent = date
    return recent.strftime("%Y-%m-%d")