from pyspark import sql
from pyspark.sql.functions import *

PATH = '/user/s2334232/project'
PATH_DEBUG = '/user/s2334232/project/part-00017-4e9879c8-68e5-4a6b-aac0-f39462b39ade-c000.json'

ss = sql.SparkSession.builder.getOrCreate()

#read data
df0=ss.read.json(PATH)

#preliminary filter
df1 = df0.filter("type == 'User' AND is_suspicious != 'true'")

#hours all users commmit at
df2 = df1.select(col('commit_list')['commit_at'].alias("commit_at"))
df2 = df2.withColumn('commit_at',explode(df2.commit_at))
print(df2.count())
df3 = df2.select(df2.commit_at.substr(12,2).alias('hour'))
df4 = df3.groupBy('hour').count()
df5 = df4.orderBy(desc("count"))
df5.show()

#hours first 10% most popular commit at
df2 = df1.select(col('commit_list')['commit_at'].alias("commit_at"),col('followers'))
df3 = df2.sort("followers", ascending=False)
df310=df3.filter('followers > 100')
print(df310.count())
#df4=df310.drop('followers')
#df4.show()
#df5= df4.withColumn('commit_at', explode(df4.commit_at))
#df5.show()
#df6 = df5.select(df5.commit_at.substr(12,2).alias('hour'))
#df6.show()
#df7 = df6.groupBy('hour').count()
#df7.show()
#df8=df7.sort("count", ascending=False)
#df8.show()
