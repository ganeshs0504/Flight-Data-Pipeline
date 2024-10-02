import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
# from pyspark.sql import types
# from pyspark.sql import functions as F

# import pandas as pd

credentials_location = "keys/datatalks-zoomcamp-99-99e0a77fa02a.json"
conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()

flights_data_url = "gs://de_zoomcamp_flight_data/2018/*"
airline_data_url = "gs://de_zoomcamp_flight_data/Airlines.csv"
output_table = "flights_data_all.flights_data"

airline_df = spark.read.csv(airline_data_url, header=True, inferSchema=True)


df = spark.read.parquet(flights_data_url)

df = df.withColumnRenamed("Operating_Airline ", "airline")
output_df = df.join(airline_df, df.airline == airline_df.Code, "inner")

output_df.createOrReplaceTempView("flights")

spark.sql("""
SELECT code, description, COUNT(*) as count
FROM flights
GROUP BY code, description
ORDER BY count DESC
LIMIT 10;
""").show()