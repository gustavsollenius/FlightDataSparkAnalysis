from pyspark.sql import *
spark = SparkSession.builder.appName("App").master("local[*]").getOrCreate()

path = "flights_data/flights.csv"
flightsdf = spark.read.csv(path,header=True,inferSchema=True)

print(flightsdf.show(20))