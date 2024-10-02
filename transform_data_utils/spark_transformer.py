import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("flight_transform") \
        .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-europe-west2-1032386787317-kyc0e7m1')

flights_data_url = "gs://de_zoomcamp_flight_data/pq/*"
airline_data_url = "gs://de_zoomcamp_flight_data/Airlines.csv"
output_table = "flights_data_all.flights_data"

airline_df = spark.read.csv(airline_data_url, header=True, inferSchema=True)


df = spark.read.parquet(flights_data_url)

df = df.withColumnRenamed("Operating_Airline ", "airline")
output_df = df.join(airline_df, df.airline == airline_df.Code, "inner")

# output_df.createOrReplaceTempView("flights")

# spark.sql("""
# SELECT code, description, COUNT(*) as count
# FROM flights
# GROUP BY code, description
# ORDER BY count DESC
# LIMIT 10;
# """).show()

output_df.write.format('bigquery') \
    .option('table', output_table) \
    .save()
