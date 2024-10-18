import pyspark
from pyspark.sql import SparkSession
import os


spark = SparkSession.builder \
        .master("local[*]") \
        .appName("flight_stat") \
        .getOrCreate()



# df = spark.read \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .csv('../raw_data/structured_data/2018/Flights_2018_1.csv')

columns_of_interest = [
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "FlightDate",
    "Flight_Number_Operating_Airline",
    "Operating_Airline ",
    "OriginAirportID",
    "Origin",
    "OriginCityName",
    "OriginState",
    "DestAirportID",
    "Dest",
    "DestCityName",
    "DestState",
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "TaxiOut",
    "TaxiIn",
    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "Cancelled",
    "CancellationCode",
    "CRSElapsedTime",
    "ActualElapsedTime",
    "AirTime"
]

parent_dir = "../raw_data/structured_data"
des_dir = "../raw_data/pq"

for fname in os.listdir(parent_dir):
    fpath = os.path.join(parent_dir, fname)

    df = spark.read.csv(fpath, header=True, inferSchema=True)
    df = df.select(*columns_of_interest)
    # print(df.count())

    final_dest_dir = os.path.join(des_dir, fname)
    # if not os.path.exists(final_dest_dir):
    #     os.makedirs(final_dest_dir)
    df.write.parquet(final_dest_dir, mode="overwrite")





