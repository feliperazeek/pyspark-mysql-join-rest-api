from jsonify import *
import datetime

def test_jsonDefault():
    date = datetime.datetime.strptime("2017-02-02", "%Y-%m-%d")
    results = jsonDefault(date)
    assert results == "2017-02-02"
