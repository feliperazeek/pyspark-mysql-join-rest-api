from dates import *

def test_formatDate():
    """
    Pytest formatDate()
    """
    import datetime
    assert formatDate(datetime.date(2007, 12, 5)) == "2007-12-05"


def test_formatDuration():
    """
    Pytest formatDuration()
    """
    assert formatDuration(59) == "59 seconds"
    assert formatDuration(90) == "1 minute, 30 seconds"
    assert formatDuration(120) == "2 minutes"
    assert formatDuration(5600) == "1 hour, 33 minutes, 20 seconds"