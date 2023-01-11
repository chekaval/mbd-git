""" //TODO to be reviewed and updated
Title: GitHub Project
[Desc. of the program's functionality and structure..]
[State how the script is supposed to be run (spark-submit args)?]

Authors: Maria Barac, Valeriia Chekanova, Dorien van Leeuwen, Hynek Noll
Runtime: [executors: 1, 3, 5, 7, 9] -> [2m10.397s, 3m4.945s, 0m59.320s, 1m5.744s, 0m54.184s]
"""

from pyspark import sql
from pyspark.sql.functions import *


# PATH = '/user/s2334232/github.json'  # the whole dataset as one file (using partitions is preferred)
PATH = '/user/s2334232/project'  #TODO to be used -> swap with the debug path
PATH_DEBUG = '/user/s2334232/project/part-00037-4e9879c8-68e5-4a6b-aac0-f39462b39ade-c000.json'  # the smallest partition

ss = sql.SparkSession.builder.getOrCreate()

df1 = ss.read.json(PATH_DEBUG)
df2 = df1.sample(.001)  # DEBUG: Take a small fraction of the dataset
df2.printSchema()  # DEBUG
print("number of data rows:", df2.count())  # DEBUG
