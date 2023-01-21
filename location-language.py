# Are there any correlations between a user’s location and the language they use in their repositories?
# Language here means programming language, maybe people in europe are more likely to use a specific programming language

from pyspark import sql
from pyspark.sql.functions import *

from datetime import datetime

from pyspark import sql
from pyspark.sql.functions import *
import re


# TODO the function does not work TypeError: 'Column' object is not callable, will be fixed if time allows
# lowercase, take the first word and remove symbols
# def normalize_location(string):
#     lower_string = string.lower()
#     words = lower_string.split()
#     if len(words[0]) < 2:
#         word = words[0] + ' ' + words[1]
#     else:
#         word = words[0]
#     return re.sub('[^a-zA-Z]+', '', word)




# PATH = '/user/s2334232/github.json'  # the whole dataset as one file (using partitions is preferred)
PATH = '/user/s2334232/project'  # TODO to be used -> swap with the debug path
PATH_DEBUG = '/user/s2334232/project/part-00037-4e9879c8-68e5-4a6b-aac0-f39462b39ade-c000.json'  # the smallest partition

ss = sql.SparkSession.builder.getOrCreate()

df1 = ss.read.json(PATH_DEBUG)
# df2 = df1.sample(.001)  # DEBUG: Take a small fraction of the dataset
# df2.printSchema()  # DEBUG

df3 = df1.select(regexp_replace(split(lower(col('location')), ' ')[0], "[^a-zA-Z0-9 ]", "").alias('location'),
                 explode((col('repo_list')['language'])).alias('language'),
                 col('login').alias('login')).dropDuplicates()
df3.show()
df4 = df3.filter(col("location").isNotNull() & col("language").isNotNull()). \
    groupby('location').count().sort(desc('count')).filter("count > 1")
df4.show()