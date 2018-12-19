import os
from dates import formatDuration
import jsonify
from pyspark.storagelevel import StorageLevel


class JobOutput:
    """
    This is gonna output the results from the job using stdout and output.txt
    """
    logger = None

    elapsedTime = None
    totalUsers = None
    totalVendorUsers = None
    totalEnriched = None
    totalMatches = None
    totalMatchesVendorActive = None
    totalMatchesInactive = None
    totalMatchesDBActive = None
    totalMatchesDBInactive = None
    totalMatchesBothActive = None
    sampleOutput = None

    showCreateTable = """
        CREATE TABLE `vendor_user` (
          `id` int(11) NOT NULL PRIMARY KEY,
          `firstname` varchar(255) DEFAULT NULL,
          `lastname` varchar(255) DEFAULT NULL,
          `practice_location` varchar(255) DEFAULT NULL,
          `specialty` varchar(50) DEFAULT NULL,
          `user_type_classification` varchar(50) DEFAULT NULL,
          `last_active_date` date DEFAULT NULL,
          `active` tinyint(1) DEFAULT NULL,
          `user_id` int(11) DEFAULT NULL,
          `user_active` tinyint(1) DEFAULT NULL,
          `user_last_active_date` date DEFAULT NULL,
          `create_date` datetime NOT NULL DEFAULT NOW(),
          FOREIGN KEY (`user_id`) REFERENCES `user` (`id`)
        ) ENGINE=InnoDB;
    """

    fileName = "output.txt"
    file = None

    def __init__(self, logger, elapsedTime, usersRDD, vendorUsersRDD, enrichedRDD):
        """
        Initializes the class that will write the output of the job to stdout and output.txt
        :param logger:
        :param elapsedTime:
        :param usersRDD:
        :param vendorUsersRDD:
        :param enrichedRDD:
        """
        self.logger = logger
        self.deleteFileIfExists()
        self.file = open(self.fileName, "a+")
        self.elapsedTime = formatDuration(elapsedTime)
        self.totalUsers = usersRDD.count()
        self.totalVendorUsers = vendorUsersRDD.count()
        self.totalEnriched = enrichedRDD.count()

        # RDD where there was a match between the db and vendor sides
        matchesRDD = enrichedRDD.filter(lambda x: x["userId"] != None).persist(StorageLevel.MEMORY_AND_DISK)

        # Sample Output
        self.sampleOutput = matchesRDD.take(10)

        # Calculate total number of records that were in the vendor side and db side
        self.totalMatches = matchesRDD.count()

        # Calculate total number of records that were in the vendor side and db size where user is active in the vendor side
        self.totalMatchesVendorActive = matchesRDD \
            .filter(lambda x: x["active"] == True) \
            .count()

        # Calculate total number of records that were in the vendor side and db size where user is inactive in the vendor side
        self.totalMatchesVendorInactive = matchesRDD \
            .filter(lambda x: x["active"] != True) \
            .count()

        # Calculate total number of records that were in the vendor side and db size where user is active in the db side
        self.totalMatchesDBActive = matchesRDD \
            .filter(lambda x: x["userActive"] == True) \
            .count()

        # Calculate total number of records that were in the vendor side and db size where user is inactive in the db side
        self.totalMatchesDBInactive = matchesRDD \
            .filter(lambda x: x["userActive"] != True) \
            .count()


    def write(self):
        """
        Write of the job to output.txt
        """
        try:
            self.append("=============================================")
            self.append("Elapsed Time: %s" % self.elapsedTime)
            self.append("=============================================")
            self.append("Total Users from Database: %s" % self.totalUsers)
            self.append("Total Users from Vendor: %s" % self.totalVendorUsers)
            self.append("Total Enriched: %s" % self.totalEnriched)
            self.append("Total Matches (db and vendor): %s" % self.totalMatches)
            self.append("Total Matches (db and vendor where active on vendor size): %s" % self.totalMatchesVendorActive)
            self.append("Total Matches (db and vendor where inactive on vendor size): %s" % self.totalMatchesVendorInactive)
            self.append("Total Matches (db and vendor where active on db size): %s" % self.totalMatchesDBActive)
            self.append("Total Matches (db and vendor where inactive on db size): %s" % self.totalMatchesDBInactive)
            self.append("=============================================")
            self.append("Sample Output:")
            for record in self.sampleOutput:
                self.append(jsonify.toJson(record))
                self.append("=============================================")
            self.append("SQL DDL:")
            self.append(self.showCreateTable)
        finally:
            self.close()


    def append(self, value):
        """
        Appends line to output.txt
        :param value:
        """
        self.logger.info(value)
        self.logger.info('\n')
        self.file.write(value)
        self.file.write('\n')


    def close(self):
        """
        Close File Handle
        :return:
        """
        self.file.close()


    def deleteFileIfExists(self):
        """
        Delete output.txt if it exists
        """
        try:
            os.remove(self.fileName)
        except OSError:
            pass
