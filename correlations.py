from pyspark import sql
from pyspark.sql.functions import *

PATH = '/user/s2334232/project'
PATH_DEBUG = '/user/s2334232/project/part-00017-4e9879c8-68e5-4a6b-aac0-f39462b39ade-c000.json'

ss = sql.SparkSession.builder.getOrCreate()
sc = ss.sparkContext

#read data
df0=ss.read.json(PATH)

#preliminary filter
df1 = df0.filter("type == 'User' AND is_suspicious != 'true'")

#calculate number of stars per user
df2 = df1.select(col('login'), col('repo_list')['stargazers_count'].alias('stargazers_count'))
df3 = df2.withColumn('stargazers_count', explode_outer(df2.stargazers_count))
df4 = df3.groupBy('login').sum()
df4 = df4.withColumnRenamed('sum(stargazers_count)','stargazers_count').filter('stargazers_count IS not null')

#calculate number of forks per user
df5 = df1.select(col('login'), col('repo_list')['forks_count'].alias('forks_count'))
df6=df5.withColumn('forks_count',explode_outer(df5.forks_count))
df7=df6.groupBy('login').sum()
df7 = df7.withColumnRenamed('sum(forks_count)','forks_count').filter('forks_count is not null')

#create df with number of followers and number of commits
df8 = df1.select(col('login'), col('followers'), col('commits'))

#join the dataframes with followers, forks and stars
dfJoinFollowersForks = df8.join(df7,['login'],"inner")
dfJoinPopularity = dfJoinFollowersForks.join(df4, ['login'], "inner")
dfJoinPopularity = dfJoinPopularity.sort("followers", ascending=False)

#calculate correlations
#all
print ('Correlations for the entire data set:'+ '\n')
a = dfJoinPopularity.corr('followers','forks_count')
print ('Correlation between number of followers and number of forks: ' + str(a) + '\n')
b = dfJoinPopularity.corr('followers','stargazers_count')
print ('Correlation between number of followers and number of stars: ' + str(b) + '\n')
d = dfJoinPopularity.corr('followers','commits')
print ('Correlation between number of followers and number of commits: ' + str(d) + '\n')

#first 10%
print('Correlations for the first 10% most followed' + '\n')
c1 = int(0.1* dfJoinPopularity.count())
dfJoinPopularity10 = dfJoinPopularity.limit(c1)
a1 = dfJoinPopularity10.corr('followers','forks_count')
print ('Correlation between number of followers and number of forks: ' + str(a1) + '\n')
b1 = dfJoinPopularity10.corr('followers','stargazers_count')
print ('Correlation between number of followers and number of stars: ' + str(b1) + '\n')
d1=dfJoinPopularity10.corr('followers','commits')
print ('Correlation between number of followers and number of commits: ' + str(d1) + '\n')

#first 1%
print('Correlations for the first 1% most followed' + '\n')
c2 = int(0.01* dfJoinPopularity.count())
dfJoinPopularity1 = dfJoinPopularity.limit(c2)
a2 = dfJoinPopularity1.corr('followers','forks_count')
print ('Correlation between number of followers and number of forks: ' + str(a2) + '\n')
b2 = dfJoinPopularity1.corr('followers','stargazers_count')
print ('Correlation between number of followers and number of stars: ' + str(b2) + '\n')
d2=dfJoinPopularity1.corr('followers','commits')
print ('Correlation between number of followers and number of commits: ' + str(d2) + '\n')









