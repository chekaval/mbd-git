""" //TODO to be reviewed and updated
Title: GitHub Project
[Desc. of the program's functionality and structure..]
[State how the script is supposed to be run (spark-submit args)?]

Authors: Maria Barac, Valeriia Chekanova, Dorien van Leeuwen, Hynek Noll
Runtime: [executors: 1, 3, 5, 7, 9] -> [2m10.397s, 3m4.945s, 0m59.320s, 1m5.744s, 0m54.184s]
"""

from pyspark import sql
from pyspark.sql.functions import *


PATH = '/user/s2334232/github.json'

ss = sql.SparkSession.builder.getOrCreate()
df1 = ss.read.json(PATH)
df1.printSchema()  # DEBUG
