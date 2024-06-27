from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import encode
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer

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


# selectionne le nombre maximal de score pour un restaurant
n = training.select(F.max(F.size('grades')).alias('n')).first().n

# On affiche le nombre maximal de note pour un restaurant
print('----------------')
print(n)
print('----------------')

# On creer nos colonnes contenant les différents score
training = training.select('borough', 'cuisine', *[F.col('grades')[i].score.alias('val_{}'.format(i+1)) for i in range(n)])

# On creer notre colonne moyenne
training = training.na.fill(value=0)
training=training.withColumn("average_grade", (col('val_1')+col('val_2')+col('val_3')+col('val_4')+col('val_5')+col('val_6')+col('val_7')+col('val_8'))/8)

# On supprime nos ancienne colonne grade
training = training.select('borough', 'cuisine', "average_grade")

# On encode nos borough et cuisine en int compréhensible pour la regression linéaire
indexer = StringIndexer(inputCol="borough", outputCol="borough_int")
training = indexer.fit(training).transform(training)

indexer = StringIndexer(inputCol="cuisine", outputCol="cuisine_int")
training = indexer.fit(training).transform(training)

# On selectionne seulement les colonnes dont on a besoin pour la regression
training = training.select('borough_int','cuisine_int','average_grade')

print(training.printSchema())
print(training.show())

# On transforme nos feature en un vecteur lisible par la regression
assembler = VectorAssembler(
    inputCols=["cuisine_int", "borough_int"],
    outputCol="features")

# On applique la transformation
df = assembler.transform(training)

# On créé notre modele de regression
lr = LinearRegression(featuresCol="features", labelCol='average_grade')

lrModel = lr.fit(df)

# On affiche les paramètres de notre modèle. 
print("Coefficients: %s" % str(lrModel.coefficients))
print("Intercept: %s" % str(lrModel.intercept))
print(lrModel.summary.rootMeanSquaredError)
print("r2: %f" % lrModel.summary.r2)
