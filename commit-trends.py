""" //TODO to be reviewed and updated
Title: GitHub Project
[desc. of this file]
Compute user commit trends over time (days since the user has joined).

Authors: Maria Barac, Valeriia Chekanova, Dorien van Leeuwen, Hynek Noll
"""
from datetime import datetime

from pyspark import sql
from pyspark.sql.functions import *
from pyspark.sql.types import *


def to_date(str_date):
    if str_date is None:
        print("NONE DATE")
        print("NONE DATE")
        print("NONE DATE")
        print("NONE DATE")
    print("str date:", str_date)
    date = datetime.strptime(str_date, "%Y-%m-%d")
    return date

# PATH = '/user/s2334232/github.json'  # the whole dataset as one file (using partitions is preferred)
PATH = '/user/s2334232/project'  # TODO to be used -> swap with the debug path
PATH_DEBUG = '/user/s2334232/project/part-00037-4e9879c8-68e5-4a6b-aac0-f39462b39ade-c000.json'  # the smallest partition
# TODO Q: The partitions appear to be smaller in total (hdfs dfs -du -h -s /user/s2334232/project) -> 44.6 G x 50 G.
#  Are the invalid rows already filtered out?

#TODO this needs to take the e['generate_at'] from each inner array to combine them into an outer array..
commit_date_udf = udf(lambda arr: [e['generate_at'] for e in arr], ArrayType(StringType()))
extract_date_udf = udf(lambda arr: [e.split()[0] for e in arr], ArrayType(StringType()))
compute_days_diff_udf = udf(lambda str_create_date, str_dates_arr: [(to_date(str_date) - to_date(str_create_date)).days
                                                                    for str_date in str_dates_arr], ArrayType(IntegerType()))
compute_days_diff_debug_udf = udf(lambda str_create_date, str_dates_arr: [str_create_date + str_date
                                                                          for str_date in str_dates_arr], ArrayType(StringType()))
# commit_date_debug_udf = udf(lambda arr: [e['generate_at'] for e in arr], ArrayType(StringType()))

ss = sql.SparkSession.builder.getOrCreate()

df1 = ss.read.json(PATH_DEBUG)
filtered = df1.filter("type == 'User' AND is_suspicious != 'true'")  # TODO rename  # filtering out non-user and suspicious accounts
df2 = filtered.sample(.001)  # DEBUG: Take a small fraction of the dataset
df2.printSchema()  # DEBUG
print("number of data rows:", df2.count())  # DEBUG
df3 = df2.select(col('id'), col('created_at'), col('commit_list'))
df3.withColumn('First_Commit', df3.commit_list.getItem(0)).show()
df4 = df3.filter(size(col('commit_list')) > 0)  # exclude users with no commits
# df4 = df3.withColumn('Commit_Dates', commit_date_udf(col('commit_list')))
# df5 = df4.select(col('id'), user_id_udf(col('commit_list')).alias('User_ID'),
#                  commit_date_udf(col('commit_list')).alias('Commit_Date'))
df5 = df4.withColumn('commit_list', commit_date_udf(col('commit_list')))\
         .withColumnRenamed('commit_list', 'commit_dates')
df6 = df5.withColumn('commit_dates', extract_date_udf(col('commit_dates')))  # extract the dates (leave out times)
df7 = df6.withColumn('created_at', udf(lambda date: date.split()[0])(col('created_at')))  # extract the date in the created_at column as well
df8 = df7.withColumn('commit_dates', compute_days_diff_udf(col('created_at'), col('commit_dates')))\
    .withColumnRenamed('commit_dates', 'commit_days_since')
df9 = df8.withColumn('commit_days_since', sort_array(col('commit_days_since')))

dftest = df7.withColumn('commit_dates', compute_days_diff_debug_udf(col('created_at'), col('commit_dates')))\
    .withColumnRenamed('commit_dates', 'commit_days_since')

df9.printSchema()
df9.show()

# def f(test):
#     print(test)
# df3.foreach(f).show()

# TODO take 'generate_at' of each commit (when was the commit created), extract the date/year?,
#  group By it and make a count of commits for each such date. average over all users
#  we can do both days and years since the user joined...

# TODO should we use the generate_at or commit_at field?? -- the first one denotes more accurately when was the work done,
#  but it might also be dated before the user account creation (local commits using git?)...
#  / We can ofc have negative values on the X-axis ('-5 days since the account creation')

