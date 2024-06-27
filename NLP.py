from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import pandas as pd
import numpy as np

#conf = pyspark.SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").setMaster("local").setAppName("ProjetBigData")

input_uri = "mongodb://127.0.0.1/mydata.rest"
output_uri = "mongodb://127.0.0.1/mydata.rest"

myspark = SparkSession\
    .builder\
    .appName("ProjetBigData")\
    .config("spark.mongodb.input.uri=mongodb://127.0.0.1/mydata.rest")\
    .config("spark.mongodb.output.uri=mongodb://127.0.0.1/mydata.rest")\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
    .getOrCreate()

training  = myspark.read.format("mongo").option("uri","mongodb://127.0.0.1/mydata.rest").load()

txtdf = training.select("name")

print(txtdf.printSchema())
print(txtdf.show())

print(txtdf.toPandas())

print(txtdf.toPandas().name.str.replace(" ",""))
txtdf.toPandas().name.str.replace(" ","").to_csv(r'file.txt', header=None, index=None)
