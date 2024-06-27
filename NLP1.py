from pyspark.sql import SparkSession
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
from sparknlp.annotator import SentenceDetectorDLModel, T5Transformer
import pandas as pd
from pyspark.sql import functions as F

myspark = SparkSession\
    .builder\
    .appName("SparkNLP")\
    .master("local[4]")\
    .config("spark.driver.memory","16G")\
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.4.0")\
    .getOrCreate()

spark = sparknlp.start()

# Création du modèle BERT

document_assembler = DocumentAssembler()\
    .setInputCol("text") \
    .setOutputCol("document")

sentence_detector = SentenceDetector()\
    .setInputCols(["document"]) \
    .setOutputCol("sentence")

embeddings = BertSentenceEmbeddings.pretrained("sent_bert_base_cased", "en") \
      .setInputCols("sentence") \
      .setOutputCol("sentence_embeddings")

nlp_pipeline = Pipeline(stages=[document_assembler, sentence_detector, embeddings])
pipeline_model = nlp_pipeline.fit(spark.createDataFrame([[""]]).toDF("text"))

path = "file.txt"
dataset = spark.read.option("header", "false").csv(path).toDF("text")
result = pipeline_model.transform(dataset)

# On récupère tous les vecteurs de nos noms de restaurant
result.select(F.col("sentence_embeddings")[0].embeddings).show()

result.select(F.col("sentence_embeddings")[0].embeddings).toPandas().to_csv(r'bert1.txt')

# On pose la question "Restaurant Italien"
path1 = "question.txt"
question = pipeline_model.transform(spark.read.option("header", "false").csv(path1).toDF("text"))

question.select(F.col("sentence_embeddings")[0].embeddings).toPandas().to_csv(r'question_vector.txt')

