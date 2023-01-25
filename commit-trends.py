"""
Title: GitHub Project - User Commit Trends
Compute user commit trends over time (days since the user has joined).
Also filter for most popular users.

Command (using client mode to be able to store output files locally with Python):
time spark-submit --master yarn --deploy-mode client --conf spark.dynamicAllocation.maxExecutors=20 commit-trends.py

Dataset Notes:
There are no accounts created in 2018.
# ids == # logins
In the debug sample:
    53735 (distinct) users in the debug sample
    13735 users with at least one commit (debug sample)
    id == commit_list.author_id
    id isn't necessarily the same as committer_id (12447 lines match, but there is also 5469 users with
    more than one unique committer_id in their commits
    there are 2192 users with some commit_at date not in generate_at (and 2058 users vice versa)
    out of 1372 filtered (10% most popular) users, only 898 have their account created before 2015,
    and 889 of them have any commits within a reasonable range
    then, there are 3511 different 'commit days' since account creation


Authors: Maria Barac, Valeriia Chekanova, Dorien van Leeuwen, Hynek Noll
Runtime: 10m0.805s
"""
import pickle
from datetime import datetime

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
PATH = '/user/s2334232/project'  # the whole dataset as 50 partitions
PATH_DEBUG = '/user/s2334232/project/part-00037-4e9879c8-68e5-4a6b-aac0-f39462b39ade-c000.json'  # the smallest partition

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
df3 = df2.select(col('id'), col('created_at'), col('commit_list'), col('followers'))
df4 = df3.filter(size(col('commit_list')) > 0)  # exclude users with no commits
# filtered_dataset_size = df4.count()  # == 2723276
# df4_popular = df4.sort(col('followers').desc()).limit(int(filtered_dataset_size/10))  # take 10% most popular users -> memory issues
df4_popular = df4.filter(col('followers') >= 100)
df5 = df4_popular.drop(col('followers')).withColumn('commit_list', commit_date_udf(col('commit_list')))\
         .withColumnRenamed('commit_list', 'commit_dates')
df6 = df5.withColumn('commit_dates', extract_date_udf(col('commit_dates')))  # extract the dates (leave out times)
df7 = df6.withColumn('created_at', udf(lambda date: date.split()[0])(col('created_at')))  # extract the date in the created_at column as well
df8 = df7.withColumn('commit_dates', expr('filter(commit_dates, e -> "2018-01-01" > e AND e >= "2005-04-07")'))  # limit the range of commit dates
df8b = df8.filter(col('created_at') < "2015-01-01")  # only user accounts at least 3 years old (there are no accounts in 2018 -> consider it the limit)
df9 = df8b.filter(size(col('commit_dates')) > 0)  # exclude users with no commits (all commits filtered) once again

# no accounts created in 2018...
# 2021-07-17 == dataset creation date
# 2005-04-07 == first git commit; source: https://marc.info/?l=git&m=117254154130732

df10 = df9.withColumn('commit_dates', compute_days_diff_udf(col('created_at'), col('commit_dates')))\
    .withColumnRenamed('commit_dates', 'commit_days_since')
df11 = df10.withColumn('commit_days_since', sort_array(col('commit_days_since')))

df12 = df11.drop('created_at')
users_count = df12.count()
# df10.select(count(col('id'))).show()  # should be the same as users_count

df13 = df12.withColumn('commit_days_since', explode('commit_days_since')).groupBy('commit_days_since').count()
df14 = df13.sort(col('commit_days_since').asc())

result = df14.collect()

# store the outputs
now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

with open("commit-trends-out/commit-trends-result-most-popular-" + now, "wb") as fp:
    pickle.dump(result, fp)
with open("commit-trends-out/commit-trends-users-count-most-popular-" + now, "wb") as fp:
    pickle.dump(users_count, fp)


# DEBUG: Exploring the dataset
# explore = df1.select('id', col('commit_list')).select('id', col('commit_list')['author_id'].alias('author_id'), col('commit_list')['committer_id'].alias('committer_id'))
# explore.filter(expr('array_contains(author_id, id)')).show()
# explore.filter(expr('array_contains(committer_id, id)')).count()
#
# explore2 = df1.select('id', col('commit_list')).select('id', col('commit_list')['commit_at'].alias('commit_at'), col('commit_list')['generate_at'].alias('generate_at'))
# explore2.withColumn('arr_diff', array_except('generate_at', 'commit_at')).where(size('arr_diff') > 0).show()
# explore2.withColumn('arr_diff', array_except('commit_at', 'generate_at')).where(size('arr_diff') > 0).show()
#
# df7.select(min('created_at')).show()  # the oldest account creation = 2007-10-20
# df7.select(max('created_at')).show()  # the newest account creation = 2017-12-31
#
# dfmindates = df7.select(array_min('commit_dates').alias('commit_dates'))
# dfmaxdates = df7.select(array_max('commit_dates').alias('commit_dates'))
#
# dfmindates.select(min('commit_dates')).show()  # the oldest commit date = 1970-01-01
# dfmaxdates.select(max('commit_dates')).show()  # the "newest" commit date = 6912-01-03
#
# dfmindates.sort(col('commit_dates').asc()).show()  # 20 oldest commit dates
# dfmaxdates.sort(col('commit_dates').desc()).show()  # 20 newest commit dates
# DEBUG END
