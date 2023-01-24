""" //TODO to be reviewed and updated
Title: GitHub Project
Compute user commit trends over time (days since the user has joined).

Command (using client mode to be able to store output files locally with Python):
time spark-submit --master yarn --deploy-mode client --conf spark.dynamicAllocation.maxExecutors=20 commit-trends.py

Dataset Notes:
There are no accounts created in 2018.
# ids == # logins
53735 (distinct) users in the debug sample


Authors: Maria Barac, Valeriia Chekanova, Dorien van Leeuwen, Hynek Noll
Runtime: 10m0.805s
"""
from datetime import datetime
import pickle

from pyspark import sql
from pyspark.sql.functions import *
from pyspark.sql.types import *


def to_date(str_date):
    date = datetime.strptime(str_date, "%Y-%m-%d")
    return date

def to_count_dict(arr):
    count_dict = {}
    for e in arr:
        count_dict[e] = count_dict.setdefault(e, 0.0) + 1
    return count_dict

# PATH = '/user/s2334232/github.json'  # the whole dataset as one file (using partitions is preferred)
PATH = '/user/s2334232/project'  # TODO to be used -> swap with the debug path
PATH_DEBUG = '/user/s2334232/project/part-00037-4e9879c8-68e5-4a6b-aac0-f39462b39ade-c000.json'  # the smallest partition
# TODO Q: The partitions appear to be smaller in total (hdfs dfs -du -h -s /user/s2334232/project) -> 44.6 G x 50 G.
#  Are the invalid rows already filtered out?

#TODO this needs to take the e['generate_at'] from each inner array to combine them into an outer array..
commit_date_udf = udf(lambda arr: [e['generate_at'] for e in arr], ArrayType(StringType()))
extract_date_udf = udf(lambda arr: [e.split()[0] for e in arr], ArrayType(StringType()))
compute_days_diff_udf = udf(lambda str_create_date, str_dates_arr:
                            [(to_date(str_date) - to_date(str_create_date)).days
                             for str_date in str_dates_arr], ArrayType(IntegerType()))
commit_days_dict_udf = udf(lambda days_arr: to_count_dict(days_arr),
                           MapType(IntegerType(), DoubleType(), False))

ss = sql.SparkSession.builder.getOrCreate()

df1 = ss.read.json(PATH)
df2 = df1.filter("type == 'User' AND is_suspicious != 'true'")  # filtering out non-user and suspicious accounts
# df2 = df_sample.sample(.001)  # DEBUG: Take a small fraction of the dataset
# df2.printSchema()  # DEBUG
df3 = df2.select(col('id'), col('created_at'), col('commit_list'))
df3.withColumn('First_Commit', df3.commit_list.getItem(0)).show()
df4 = df3.filter(size(col('commit_list')) > 0)  # exclude users with no commits
df5 = df4.withColumn('commit_list', commit_date_udf(col('commit_list')))\
         .withColumnRenamed('commit_list', 'commit_dates')
df6 = df5.withColumn('commit_dates', extract_date_udf(col('commit_dates')))  # extract the dates (leave out times)
df7 = df6.withColumn('created_at', udf(lambda date: date.split()[0])(col('created_at')))  # extract the date in the created_at column as well
#TODO run with additional filters! - maxDate, minDate...
df8 = df7.withColumn('commit_dates', expr('filter(commit_dates, e -> "2018-01-01" > e AND e >= "2005-04-07")'))
df8b = df8.filter(col('created_at') < "2015-01-01")  # only user accounts at least 3 years old (there are no accounts in 2018 -> consider it the limit)
df9 = df8b.filter(size(col('commit_dates')) > 0)  # exclude users with no commits once again

# no accounts created in 2018...
# 2021-07-17 == dataset creation date
# 2005-04-07 == first git commit
# source: https://marc.info/?l=git&m=117254154130732

df10 = df9.withColumn('commit_dates', compute_days_diff_udf(col('created_at'), col('commit_dates')))\
    .withColumnRenamed('commit_dates', 'commit_days_since')
df11 = df10.withColumn('commit_days_since', sort_array(col('commit_days_since')))

# TODO make a dictionary of counts of commit_days_since (for each row?), then merge for all users, divide by count
#  (make an average)
df12 = df11.drop('created_at')
users_count = df12.count()
df10.select(count(col('id'))).show()  # should be the same

# df11 = df10.agg(flatten(collect_list('commit_days_since')).alias('commit_days_since'))
# df12 = df11.withColumn('commit_days_since', commit_days_dict_udf(col('commit_days_since'), df10.count('commit_days_since')))

df13 = df12.withColumn('commit_days_since', explode('commit_days_since')).groupBy('commit_days_since').count()
df14 = df13.sort(col('commit_days_since').asc())

df14.printSchema()
df14.show()

result = df14.collect()

df7.select(min('created_at')).show()  # the oldest account creation = 2007-10-20
df7.select(max('created_at')).show()  # the newest account creation = 2017-12-31

dfmindates = df7.select(array_min('commit_dates').alias('commit_dates'))
dfmaxdates = df7.select(array_max('commit_dates').alias('commit_dates'))

dfmindates.select(min('commit_dates')).show()  # the oldest commit date = 1970-01-01
dfmaxdates.select(max('commit_dates')).show()  # the "newest" commit date = 6912-01-03

dfmindates.sort(col('commit_dates').asc()).show()  # 20 oldest commit dates
dfmaxdates.sort(col('commit_dates').desc()).show()  # 20 newest commit dates

# store the outputs
now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

with open("commit-trends-out/commit-trends-result-" + now, "wb") as fp:
    pickle.dump(result, fp)
with open("commit-trends-out/commit-trends-users-count-" + now, "wb") as fp:
    pickle.dump(users_count, fp)

# def f(test):
#     print(test)
# df3.foreach(f).show()

# TODO take 'generate_at' of each commit (when was the commit created), extract the date/year?,
#  group By it and make a count of commits for each such date. average over all users
#  we can do both days and years since the user joined...

# TODO should we use the generate_at or commit_at field?? -- the first one denotes more accurately when was the work done,
#  but it might also be dated before the user account creation (local commits using git?)...
#  / We can ofc have negative values on the X-axis ('-5 days since the account creation')

# TODO run on the cluster, make a graph from the whole dataset, measure times, try different configs?


#TODO explain the oscillation

#TODO we could compare most popular users' trends vs least popular / random sample / the rest...

# TODO the commits before account creation might be due to forking??
