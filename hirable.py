from pyspark import sql
from pyspark.sql.functions import *

PATH = '/user/s2334232/project'
PATH_DEBUG = '/user/s2334232/project/part-00004*'

ss = sql.SparkSession.builder.getOrCreate()

df1 = ss.read.json(PATH)

filtered = df1.filter(("company IS NULL AND is_suspicious != 'true'"))

# IQR between 1 and 13, thus high outliers above 13 + 1,5 * 12 = 31
hirable = filtered.select('hirable', 'public_repos').filter('hirable == true AND public_repos < 32')
not_hirable = filtered.select('hirable', 'public_repos').filter('hirable IS NULL AND public_repos < 32')

stats_hirable = hirable.summary()
stats_not_hirable = not_hirable.summary()

stats_hirable.write.json('results_hirable_limited')
stats_not_hirable.write.json('results_not_hirable_limited')



