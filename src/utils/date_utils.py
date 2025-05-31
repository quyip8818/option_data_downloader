from datetime import timedelta


def get_last_workday(date):
    if date.weekday() == 5:  # Saturday
        return date - timedelta(days=1)
    elif date.weekday() == 6:  # Sunday
        return date - timedelta(days=2)
    else:
        return date
