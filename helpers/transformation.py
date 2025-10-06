import re


def split_date(date_str):

    """Regex to extract weekday, day, and month from raw extracted data"""

    match = re.match(r'(\w+), (\d+)\s+(\w+)', date_str)
    if match:
        weekday, day, month = match.groups()
        return weekday, int(day), month
    return None, None, None