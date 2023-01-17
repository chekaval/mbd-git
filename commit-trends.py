""" //TODO to be reviewed and updated
Title: GitHub Project
[desc. of this file]
Compute user commit trends over time (days since the user has joined).

Authors: Maria Barac, Valeriia Chekanova, Dorien van Leeuwen, Hynek Noll
"""

from pyspark import sql
from pyspark.sql.functions import *

# PATH = '/user/s2334232/github.json'  # the whole dataset as one file (using partitions is preferred)
PATH = '/user/s2334232/project'  # TODO to be used -> swap with the debug path
PATH_DEBUG = '/user/s2334232/project/part-00037-4e9879c8-68e5-4a6b-aac0-f39462b39ade-c000.json'  # the smallest partition
# TODO Q: The partitions appear to be smaller in total (hdfs dfs -du -h -s /user/s2334232/project) -> 44.6 G x 50 G.
#  Are the invalid rows already filtered out?

commit_date_udf = udf(lambda arr: [e['generate_at'] for e in arr] if arr is not None else arr)
user_id_udf = udf(lambda arr: [e['author_id'] for e in arr] if arr is not None else arr)

ss = sql.SparkSession.builder.getOrCreate()

df1 = ss.read.json(PATH_DEBUG)
filtered = df1.filter("company IS NULL AND is_suspicious != 'true'")  # TODO rename
df2 = filtered.sample(.001)  # DEBUG: Take a small fraction of the dataset
df2.printSchema()  # DEBUG
print("number of data rows:", df2.count())  # DEBUG
df3 = df2.select(col('id'), col('created_at'), col('commit_list'))
df3.withColumn('First_Commit', df3.commit_list.getItem(0)).show()
df4 = df3.filter(size(col('commit_list')) > 0)  # exclude users with no commits
# df4 = df3.withColumn('Commit_Dates', commit_date_udf(col('commit_list')))
# df5 = df4.select(col('id'), user_id_udf(col('commit_list')).alias('User_ID'),
#                  commit_date_udf(col('commit_list')).alias('Commit_Date'))
df5 = df4.withColumn('Commit_Date', commit_date_udf(col('commit_list')))


# df3.foreach(lambda f: {
#             print(f)
# }).show()

# TODO take 'generate_at' of each commit (when was the commit created), extract the date/year?,
#  group By it and make a count of commits for each such date. average over all users

# TODO should we use the generate_at or commit_at field?? -- the first one denotes more accurately when was the work done,
#  but it might also be dated before the user account creation (local commits using git?)...
#  / We can ofc have negative values on the X-axis ('-5 days since the account creation')

