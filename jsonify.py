import json
from dates import *
import datetime

def jsonDefault(o):
    """
    When doing a json dump, this function is passed default to convert datetime instances to yyyy-mm-dd
    :param o:
    :return: date formatted string
    """
    if type(o) is datetime.date or type(o) is datetime.datetime:
        return formatDate(o)
    else:
        raise TypeError("Unserializable object {} of type {}".format(o, type(o)))


def toJson(x):
    """
    Object Instance to JSON
    :param x:
    :return: json string from object instance
    """
    return json.dumps(x, default=jsonDefault, sort_keys=True, indent=4)
