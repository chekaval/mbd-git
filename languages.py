from pyspark import sql
from pyspark.sql.functions import *

PATH = '/user/s2334232/project'
PATH_DEBUG = '/user/s2334232/project/part-00017-4e9879c8-68e5-4a6b-aac0-f39462b39ade-c000.json'

ss = sql.SparkSession.builder.getOrCreate()

df1 = ss.read.json(PATH)

#preliminary filter
df1 = df1.filter("type == 'User' AND is_suspicious != 'true'")

#most used programming languages for all users
df2 = df1.select(col('repo_list')['language'].alias('languages'))
df2 = df2.withColumn('languages', explode(df2.languages))
df3 = df2.groupBy('languages').count()
df3 = df3.orderBy(desc("count"))
df3.show()

#most used programming languages for top 10% users
df4 = df1.select(col('followers'),col('repo_list')['language'].alias('language'))
df4 = df4.orderBy(desc("followers"))
a = int(0.1*df4.count())
df5 = df4.limit(a)
df6 = df5.withColumn('language', explode(df5.language))
df6 = df6.groupBy('language').count()
df6=df6.orderBy(desc("count"))
df6.show()

