from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import pandas as pd
import streamlit as st

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

posdata = training.select(F.col('address').coordinates[0].alias('lon'),F.col('address').coordinates[1].alias('lat'))

print(posdata.printSchema())
print(posdata.show())

print(posdata.toPandas())

st.map(posdata.toPandas())




