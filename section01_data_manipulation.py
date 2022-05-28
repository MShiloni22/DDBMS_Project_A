# Current file = queries.csv
# Transform the file to tsv format
import csv

with open('queries.csv','r') as csvin, open('queries.tsv', 'w') as tsvout:
    csvin = csv.reader(csvin)
    tsvout = csv.writer(tsvout, delimiter='\t')
    for row in csvin:
        tsvout.writerow(row)

from pyspark.sql import SparkSession,Row, Column
import pyspark.sql.functions as F
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
queries_file = 'queries.tsv'
df = spark.read.csv(queries_file, header='True', inferSchema='True', sep='\t')


column_names = ["genres", "lang", "actors", "director", "cities", "country",
                "from_realese_date", "production_company"]
# For all the above columns
for name in column_names:
  temp_name_1 = name + "1"
  temp_name_2 = name + "2"
  # Removing irrelevant chars
  df = df.select("*", F.translate(F.col(name), "'[]", "")\
                .alias(temp_name_1))\
  .drop(name)

  # Converting arrays strings to arrays of strings
  df = df.select("*", F.split(F.col(temp_name_1),",").alias(temp_name_2)) \
      .drop(temp_name_1)
  df = df.withColumnRenamed(temp_name_2,name)

print("Queries table:")
df.show()
queries_df = df

# ------------------------------------------------------------------------------

# Current file: credits.csv
import re
credits_file = 'credits.csv'
df = spark.read.csv(credits_file, header='True', inferSchema='True')

# load the data as you did before,
# just now change the delimiter to get evreything together
credits = spark.read.format("csv")\
.option("delimiter", "\t")\
.option("header","true")\
.option("inferSchema", "true")\
.load("credits.csv")
prog = re.compile('\\[(.*?)\\]')
second_match = F.udf(lambda x: prog.findall(x)[1])
id_extract = F.udf(lambda x: x.split(",")[-1])
credits = credits\
.withColumn("id", id_extract("cast,crew,id"))\
.withColumn("cast", F.regexp_extract(F.col("cast,crew,id"), '\\[(.*?)\\]', 0
))\
.withColumn("crew", F.concat(F.lit("["),second_match("cast,crew,id"), F.lit(
"]")))\
.select("cast", "crew", "id")
df = credits
# df.printSchema()
column_names = ["cast", "crew"]
# For all the above columns
for name in column_names:
  temp_name_1 = name + "1"
  temp_name_2 = name + "2"
  # Removing irrelevant chars
  df = df.select("*", F.translate(F.col(name), "\\{\\[\\]'\\}", "")\
                .alias(temp_name_1))\
  .drop(name)

  # Converting arrays strings to arrays of strings
  df = df.select("*", F.split(F.col(temp_name_1),",").alias(temp_name_2)) \
      .drop(temp_name_1)
  df = df.withColumnRenamed(temp_name_2,name)

# For cast column - udf for extracting actors' names only from cast json string
actors_udf = F.udf(lambda arr: [arr[i][7:] for i in range(len(arr)) if i % 8 == 5])
df = df.withColumn('actors', actors_udf(F.col("cast")))\
  .drop("cast")

# For crew column - udf for extracting directors' names only from crew json string
directors_udf = F.udf(lambda arr: [arr[i+1][7:] for i in range(len(arr))
 if arr[i] == " job: Director"])
df = df.withColumn('directors', directors_udf(F.col("crew")))\
  .drop("crew")

# Converting arrays strings to arrays of strings
column_names = ["actors", "directors"]
for name in column_names:
  temp_name_1 = name + "1"
  temp_name_2 = name + "2"
  # Removing irrelevant chars
  df = df.select("*", F.translate(F.col(name), "\\{\\[\\]'\\}", "")\
                .alias(temp_name_1))\
  .drop(name)

  # Converting arrays strings to arrays of strings
  df = df.select("*", F.split(F.col(temp_name_1),",").alias(temp_name_2)) \
      .drop(temp_name_1)
  df = df.withColumnRenamed(temp_name_2,name)

print("Credits table:")
df.show()
credits_df = df

# ------------------------------------------------------------------------------

# Current file: movies.csv
movies_file = 'movies.csv'
df = spark.read.csv(movies_file, header='True', inferSchema='True')

# Doing the same process for all columns
column_names = ["genres", "production_companies", "production_countries",
                "spoken_languages", "cities"]
for name in column_names:
  temp_name_1 = name + "1"
  temp_name_2 = name + "2"
  # Removing irrelevant chars
  df = df.select("*", F.translate(F.col(name), "\\{\\[\\]'\\}", "")\
                .alias(temp_name_1))\
  .drop(name)

  # Converting arrays strings to arrays of strings
  df = df.select("*", F.split(F.col(temp_name_1),",").alias(temp_name_2)) \
      .drop(temp_name_1)
  df = df.withColumnRenamed(temp_name_2,name)

# Finished working on cities column, and seperating production_companies
# because it has different structure
column_names = ["genres", "production_countries", "spoken_languages"]

# For each column - udf for extracting names only from json string
name_udf = F.udf(lambda arr: [arr[i][7:] for i in range(len(arr)) if i % 2 == 1])
for c in column_names:
  c_1 = c + "1"
  df = df.withColumn(c_1, name_udf(F.col(c)))\
    .drop(c)
prod_udf = F.udf(lambda arr: [arr[i][6:] for i in range(len(arr)) if i % 2 == 0])
df = df.withColumn("production_companies1", prod_udf(F.col("production_companies")))\
  .drop("production_companies")

# Renameing columns names
column_names = ["genres", "production_companies", "production_countries",
                "spoken_languages"]
for name in column_names:
  current_name = name + "1"
  df = df.withColumnRenamed(current_name,name)

print("Movies table:")
df.show()
movies_df = df
