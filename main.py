from pyspark.sql import *
spark = SparkSession.builder.appName("App").master("local[*]").getOrCreate()
spark.sp
path = "flights_data/flights.csv"
flightsdf = spark.read.csv(path,header=True,inferSchema=True)

print(flightsdf.filter(flightsdf.CANCELLED != 0).show(10))