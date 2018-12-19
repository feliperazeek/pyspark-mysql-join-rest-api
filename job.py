import multiprocessing
import time

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import sql
from pyspark.storagelevel import StorageLevel

import vendor
from models import UserReference
from output import JobOutput


def run(sc, logger):
    """
    Run spark job that will associate vendor's users (loaded from REST API) with our users (loaded from MySQL)
    :param sc: spark context
    :param logger
    """
    start = time.time()

    # Spark SQL Context
    sqlContext = sql.context.SQLContext(sc)

    # 1) Practices - Small table so no need to partition it, could even broadcast across all executors
    # Spark SQL JDBC Options: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    practicesRDD = getPracticesRDD(sqlContext)

    # 1) Debug Practices
    logger.info("-------------")
    logger.info("1a) Practices RDD: %s (%s partition(s))" % (practicesRDD, practicesRDD.getNumPartitions()))
    logger.info("1b) Count Practices: %s \n" % practicesRDD.count())

    # 2) User Table - Big table so need to read in batches, so partition the table by id
    # Spark SQL JDBC Options: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    usersRDD = getUsersRDD(sqlContext)

    # 2) Debug Users
    logger.info("-------------")
    logger.info("2a) Users RDD: %s (%s partition(s))" % (usersRDD, usersRDD.getNumPartitions()))
    logger.info("2b) Count Users: %s \n" % usersRDD.count())

    # 3) Join Users and Practices (user.practice_id = user_practice.id)
    joinedUsersRDD = usersRDD \
        .join(practicesRDD) \
        .persist(StorageLevel.MEMORY_AND_DISK) \
        .map(lambda row: ((row[1][0].firstname, row[1][0].lastname, row[1][1].location, row[1][0].specialty), UserReference(row[1][0].id, row[1][0].last_active_date)))

    # 3) Debug Joined Users
    logger.info("-------------")
    logger.info("3a) Joined Users RDD: %s (%s partition(s))" % (joinedUsersRDD, joinedUsersRDD.getNumPartitions()))
    logger.info("3b) Count Joined Users: %s \n" % joinedUsersRDD.count())

    # 4) Vendor Users
    vendorUsersRDD = vendor.getVendorUsers(sc) \
        .persist(StorageLevel.MEMORY_AND_DISK) \
        .map(lambda row: ((row["firstName"], row["lastName"], row["practiceLocation"], row["specialty"]), row))

    # 4) Debug Vendor Users
    logger.info("-------------")
    logger.info("4a) Vendor Users RDD: %s (%s partition(s))" % (vendorUsersRDD, vendorUsersRDD.getNumPartitions()))
    logger.info("4b) Count Vendor Users: %s \n" % vendorUsersRDD.count())

    # 4) Enriched Users (associate our user record with the vendor user)
    # Since we already have our users loaded in our db, just gonna load the vendor user with a pointer to our user
    enrichedRDD = joinedUsersRDD \
        .rightOuterJoin(vendorUsersRDD) \
        .persist(StorageLevel.MEMORY_AND_DISK) \
        .map(lambda row: enrich(row))

    # 5) Debug Enriched Users
    logger.info("-------------")
    logger.info("5a) Enriched Users RDD: %s (%s partition(s))" % (enrichedRDD, enrichedRDD.getNumPartitions()))
    logger.info("5b) Count Enriched Users: %s \n" % enrichedRDD.count())

    # 6) Save to disk in JSON
    logger.info("-------------")

    # Calculate how long it took for the job to run
    end = time.time()
    elapsed = end - start

    # Job Output (write to stdout and output.txt)
    output = JobOutput(logger, elapsed, usersRDD, vendorUsersRDD, enrichedRDD)
    output.write()


def getUsersRDD(sqlContext):
    """
    DB Table (user) RDD
    :param sqlContext:
    :return: users rdd
    """
    # Currently the id field ranges from '0' to '1000000'.
    # To avoid loading it all in memory, partition on the id field (100 partitions, about 10k records per partition).
    # Also setting fetch size to 10,000 to avoid multiple database calls per partition.
    # All records from a single partition will come in a single query.
    # If we need to use less memory, we can increase the # of partitions and decrease the lower/uppper bounds.
    # We are also relying on Spark to spill to disk if no memory is available.
    from db import *
    return sqlContext \
        .read \
        .format("jdbc") \
        .options(
            driver=driver,
            url=url,
            dbtable="user",
            user=user,
            password=password,
            fetchSize=10000,
            numPartitions=100,
            partitionColumn="id",
            lowerBound=0,
            upperBound=1000000
        ) \
        .load() \
        .rdd \
        .persist(StorageLevel.MEMORY_AND_DISK) \
        .map(lambda row: (row.practice_id, row)) # We are setting practice_id as the key here because we'll use that to join with user_practice table


def getPracticesRDD(sqlContext):
    """
    DB Table (user_practice) RDD
    :param sqlContext:
    :return: practices rdd
    """
    from db import *
    return sqlContext \
        .read \
        .format("jdbc") \
        .options(
            driver=driver,
            url=url,
            dbtable="user_practice",
            user=user,
            password=password
        ) \
        .load() \
        .rdd \
        .map(lambda row: (row.id, row)) # We'll use the id to join with the users rdd defined above


def enrich(r):
    """
    Set userId on final rdd
    :param r:
    :return: enriched rdd
    """
    row = r[1]
    userReference = row[0]
    user = row[1]
    # if there's no user, it means there wasn't match between the users in the db vs the ones from the vendor coming from the api
    if user and userReference:
        user["userId"] = userReference.userId
        user["userActive"] = userReference.active
        user["userLastActiveDate"] = userReference.lastActiveDate
    return user


if __name__ == "__main__":
    name = "UsersJob"
    cpus = multiprocessing.cpu_count()

    sparkConf = SparkConf() \
            .setAppName(name) \
            .set("spark.executor.memory", "512m") \
            .set("spark.driver.memory", "512m") \
            .set("spark.executor.cores", cpus) \
            .set("spark.driver.cores", cpus)

    sc = SparkContext(conf=sparkConf)

    # Init Spark Logger
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(name)

    try:
        run(sc, logger)

    finally:
        sc.stop()

