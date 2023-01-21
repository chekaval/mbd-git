# Are there any correlations between a user’s location and the language they use in their repositories?
# Language here means programming language, maybe people in europe are more likely to use a specific programming language

from pyspark import sql
from pyspark.sql.functions import *

from datetime import datetime

from pyspark import sql
from pyspark.sql.functions import *
import re


# TODO the function does not work TypeError: 'Column' object is not callable, will be fixed if time allows

# lowercase, take the first word and remove symbols except a whitespace
# if the first word is new or san keep first and second words, otherwise only thr first one
def normalize_location(string):
    lower_string = string.lower()
    words = lower_string.split()
    if words[0] == "san" or words[0].lower() == "new":
        return " ".join(string.split()[:2])
    else:
        return " ".join(string.split()[:1])


# PATH = '/user/s2334232/github.json'  # the whole dataset as one file (using partitions is preferred)
PATH = '/user/s2334232/project'  # TODO to be used -> swap with the debug path
PATH_DEBUG = '/user/s2334232/project/part-00037-4e9879c8-68e5-4a6b-aac0-f39462b39ade-c000.json'  # the smallest partition

ss = sql.SparkSession.builder.getOrCreate()

df1 = ss.read.json(PATH_DEBUG)

df3 = df1.select(regexp_replace(split(lower(col('location')), ' ')[0], "[^a-zA-Z0-9 ]", "").alias('location'),
                 # TODO it's not clear |         |       Java|  134| why do we have an empty location like this,
                 # TODO what symbols does it contain?
                 explode((col('repo_list')['language'])).alias('language'),
                 col('login').alias('login'))
# TODO check that is not counted as | aus | 2 |
# |     aus|       PHP|    adrivar0909|
# |     aus|       PHP|    adrivar0909|
#     # .dropDuplicates(["language", "login"]) <- not sure if it workds
df4 = df3.filter(col("location").isNotNull() & col("language").isNotNull()). \
    groupby(['location', 'language']).count().sort(desc('count')).filter("count > 1")

# +---------+-----------+-----+
# | location|   language|count|
# +---------+-----------+-----+
# |   austin|       Ruby|  305|
# |  rosario| JavaScript|  286|
# |   tehran|      Swift|  254|
# |      san| JavaScript|  206|
# | enumclaw|       Ruby|  195|
# |  rosario|        PHP|  188|
# |      new| JavaScript|  169|
# |    paris| JavaScript|  166|
# | waterloo|        C++|  144|
# |   united|       Java|  140|
# |         |       Java|  134|

# TODO more often than not we get cities in the first column.
# TODO would be nice to replace it by the contry name and then count it.
# TODO so Retiro (Argentina) | Java | 20 and Quilmes (Argentina) | Java | 30 results in Argentina | Java | 50

df4.show()
