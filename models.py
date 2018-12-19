import datetime

# Requirement - Assume current date is '2017-02-02'
today = datetime.datetime.strptime("2017-02-02", "%Y-%m-%d")

# Any user whose last active date is before this date is considered inactive
lastActiveDate = today + datetime.timedelta(-30)

class User:
    """
    Vendor User
    """
    id = None
    firstName = None
    lastName = None
    practiceLocation = None
    specialty = None
    userTypeClassification = None
    lastActiveDate = None
    active = False

    # Not including the rest of the db user fields because they can be be joined using the user's table keyed by this userId and the primary key on that table
    userId = None
    userActive = None
    userLastActiveDate = None

    def __init__(self, json):
        """
        Initialize an instance of an user (coming from Vendor's REST API) without userId populated (which will be enriched by Spark job later)
        :param json: json returned by vendor's rest api
        """
        self.id = json.get("id")
        self.firstName = json.get("firstname")
        self.lastName = json.get("lastname")
        self.practiceLocation = json.get("practice_location")
        self.specialty = json.get("specialty")
        self.userTypeClassification = json.get("user_type_classification")
        self.userId = None
        self.lastActiveDate = datetime.datetime.strptime(json.get("last_active_date"), "%Y-%m-%d")
        if self.lastActiveDate:
            self.active = self.lastActiveDate > lastActiveDate


class UserReference:
    """
    User information
    """
    userId = None
    lastActiveDate = None
    active = None

    def __init__(self, uid, date):
        self.userId  = uid
        self.lastActiveDate = datetime.datetime.strptime(date.strftime("%Y-%m-%d"), "%Y-%m-%d")
        if self.lastActiveDate:
            self.active = self.lastActiveDate > lastActiveDate



