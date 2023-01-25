"""
Title: GitHub Project - Correlation between Bio Length and the Number of Followers

Authors: Maria Barac, Valeriia Chekanova, Dorien van Leeuwen, Hynek Noll
"""
from pyspark import sql
from pyspark.sql.functions import *

PATH = '/user/s2334232/project'
PATH_DEBUG = '/user/s2334232/project/part-00004*'

ss = sql.SparkSession.builder.getOrCreate()

df1 = ss.read.json(PATH)

filtered = df1.filter(("type == 'User' AND is_suspicious != 'true'"))

bio = filtered.select('bio', 'followers', 'following').filter('bio IS NOT NULL') \
    .withColumn('length_bio', length('bio')) \
    .withColumn('bucket_bio', when(col('length_bio') <= 40, 1).when(col('length_bio') <= 80, 2).when(col('length_bio') <= 120, 3).otherwise(4))

result = bio.groupBy('bucket_bio').agg({"followers": "avg", "following": "avg", "bucket_bio": "count"})

result.write.json('results_bio_follow')
