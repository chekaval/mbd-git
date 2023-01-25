"""
Title: GitHub Project - Relation between Hirable Status and the Number of Public Repositories

Authors: Maria Barac, Valeriia Chekanova, Dorien van Leeuwen, Hynek Noll
"""
from pyspark import sql
from pyspark.sql.functions import *

PATH = '/user/s2334232/project'
PATH_DEBUG = '/user/s2334232/project/part-00004*'

ss = sql.SparkSession.builder.getOrCreate()

df1 = ss.read.json(PATH)

filtered = df1.filter(("type == 'User' AND is_suspicious != 'true'"))

hirable = filtered.select('hirable', 'public_repos').filter('hirable == true')
not_hirable = filtered.select('hirable', 'public_repos').filter('hirable IS NULL')

stats_hirable = hirable.summary()
stats_not_hirable = not_hirable.summary()

stats_hirable.write.json('results_hirable')
stats_not_hirable.write.json('results_not_hirable')



