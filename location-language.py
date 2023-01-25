from pyspark import sql
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import codecs

sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)
from datetime import datetime

from pyspark import sql
from pyspark.sql.functions import *
import json
import re

# PATH_DEBUG = '/user/s2334232/github.json'  # the whole dataset as one file (using partitions is preferred)
PATH_CITIES = '/home/s3094650/cities.json'
PATH_DEBUG = '/user/s2334232/project/part-00037-4e9879c8-68e5-4a6b-aac0-f39462b39ade-c000.json'  # the smallest partition

cities = {'les', 'saint', 'phumi', 'goth', 'hacienda', 'al', 'qal`eh-ye', 'haji', 'south', 'dar', 'oulad', 'villa',
          'banjar', 'estancia', 'santa', 'west', 'kampong', 'north', '`izbat', 'le', 'wan', 'new', 'deh-e', 'xom', 'el',
          'ap', 'kafr', 'deh', 'bolshaya', 'sao', 'east', 'lang', 'las', 'ait', 'phum', 'novaya', 'basti', 'caserio',
          'sitio', 'puerto', 'kampung', 'chefe', 'rancho', 'la', 'de', 'chak', 'san', 'los', 'ban', 'douar'}


# lowercase, take the first word and remove symbols except a whitespace
# if the first words is a city prefix (is in set of prefixes we got from the general cities data) we keep 2 words
# otherwise we keep first word
def normalize_location(string):
    a = string.lower()
    words = re.split(', | |_|-|!', a)
    if len(words) > 1 and words[0] in cities:
        first_two = words[0] + " " + words[1]
        return " ".join(re.findall(r"[a-zA-Z0-9]+", first_two))
    else:
        return words[0]


def load_cities():
    with open(PATH_CITIES) as f:
        return json.load(f)


def replace_city_with_country(city):
    our_key = [key
               for key, list_of_values in country_cities_dic.items()
               if city in list_of_values]
    if our_key:
        return our_key[0]
    else:
        return city


country_cities_dic = load_cities()

ss = sql.SparkSession.builder.getOrCreate()
parse_string_udf = udf(lambda str_location: normalize_location(str_location), StringType())
replace_city_with_country_udf = udf(lambda str_location: replace_city_with_country(str_location), StringType())

df1 = ss.read.json(PATH_DEBUG)
df2 = df1.filter("type == 'User' AND is_suspicious != 'true'")
df3 = df2.select(col('location').alias('location'),
                 explode((col('repo_list')['language'])).alias('language'),
                 col('login').alias('login'))
df4 = df3.filter(col("location").isNotNull() & col("language").isNotNull())
df5 = df4.withColumn('location', parse_string_udf(col('location')))
df6 = df5.withColumn('location', replace_city_with_country_udf(col('location')))
df7 = df6.groupby(['location', 'language']).count().sort(desc('count')).filter("count > 1")
df7.show(100)
# windowDept = Window.partitionBy('location').orderBy(col('count').desc())
# df9 = df7.withColumn("row", row_number().over(windowDept))
# df10 = df9.filter(col("row") <= 3).drop("row")
# df10.show(50)
