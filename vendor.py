import requests
from models import User

# Vendor URL
url = "http://de-tech-challenge-api.herokuapp.com/api/v1/users"

def getTotalPages():
    """
    Find the total number of pages that are available in the Vendor's REST API
    :return: total number pages
    """
    r = requests.get(url)
    total = int(r.json().get('total_pages'))
    return total


def getUsers(i):
    """
    Get list of users from the vendor for a specific page
    :param i: page number
    :return: list of vendor users
    """
    r = requests.get("%s?page=%s" % (url, i))
    json = r.json()
    users = json.get("users")
    results = []
    for u in users:
        user = User(u)
        row = (vars(user), user)
        results.append(row)
    return results


def getVendorUsers(sc):
    """
    Get RDD for Vendor Users
    :param sc: spark context
    :return: vendor users rdd loaded from rest api
    """
    return sc.parallelize(xrange(getTotalPages() + 1)) \
        .map(lambda row: getUsers(row)) \
        .flatMap(lambda xs: [x[0] for x in xs])

