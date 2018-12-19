
# MySQL Parameters
# TODO use command line arguments and store password in a safer way
driver      = "com.mysql.jdbc.Driver"
host        = "....."
user        = "......"
password    = "...."
port        = 3316
schema      = "...."
url         = "jdbc:mysql://%s:%s/%s" % (host, port, schema)
