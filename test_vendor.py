from vendor import *

def test_total():
    assert getTotalPages() > 0


def test_users():
    assert len(getUsers(1)) > 0