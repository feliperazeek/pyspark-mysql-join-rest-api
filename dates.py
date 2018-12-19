import re

def formatDuration(seconds):
    """
    User friendly duration string like (X minutes, X seconds)
    :param seconds:
    :return:
    """
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    weeks, days = divmod(days, 7)
    str = "%d weeks, %d days, %d hours, %d minutes, %d seconds" % (weeks, days, hours, minutes, seconds)
    str = re.sub("(?<![1-9])0 [a-z]+,? ?", "", str).strip()  # Removing 0's
    str = re.sub(", ?$", "", str)  # Removing bogus commas
    str = re.sub("(?<![0-9])1 ([a-z]+)s", "1 \\1", str)  # Non-pluralizing 1's
    return str


def formatDate(date):
    """
    Format date to serialized in JSON
    :param date:
    :return: date formatted string
    """
    return date.strftime("%Y-%m-%d")


def test_formatDate():
    """
    Pytest formatDate()
    """
    import datetime
    assert formatDate(datetime.date(2007, 12, 5)) == "2007-12-05"