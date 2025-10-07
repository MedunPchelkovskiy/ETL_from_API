import re


def split_date(date_str):

    """Regex to extract weekday, day, and month from raw extracted data"""


    # Use regex to split by spaces, commas, semicolons, or other delimiters
    substrings = re.split(r'[,\s;]+', date_str)


    # match = re.match(r'(\w+), (\d+)\s+(\w+)', date_str)
    if len(substrings) == 3:
        weekday, day, month = substrings
        return weekday, int(day), month
    elif len(substrings) == 2:
        day, month = substrings
        return int(day), month
    return None, None, None