from pyspark.sql import SparkSession

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



df = myspark.read.format("mongo").option("uri","mongodb://127.0.0.1/mydata.rest").load()

print(df.show())