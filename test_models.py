from models import UserReference
import datetime

def test_active():
    """
    Test the active flag that gets set on the constructor of UserReference
    :return:
    """
    assert UserReference("uid", datetime.date(2007, 12, 5)).active == False
    assert UserReference("uid", datetime.date(2017, 1, 1)).active == False
    assert UserReference("uid", datetime.date(2016, 12, 5)).active == False
    assert UserReference("uid", datetime.date(2017, 2, 1)).active == True
    assert UserReference("uid", datetime.date(2017, 1, 15)).active == True
    assert UserReference("uid", datetime.date(2017, 1, 4)).active == True