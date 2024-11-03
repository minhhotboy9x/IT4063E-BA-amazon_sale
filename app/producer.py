import os

import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from schemas import AMAZON_SCHEMA
from config import CONNECTION_STRING, MASTER, ES_NODES, AMAZON_TOPIC, KAFKA_BROKER1
from pymongo import MongoClient
import time

print(findspark.init())
os.environ["PYSPARK_PYTHON"] = "python"
print("SPARK_HOME:", os.environ.get('SPARK_HOME'))
print("PYTHONPATH:", os.environ.get('PYTHONPATH'))
print("HADOOP_HOME:", os.environ.get('HADOOP_HOME'))
print("JAVA_HOME:", os.environ.get('JAVA_HOME'))
print(f"PySpark version: {pyspark.__version__}")


client = MongoClient(CONNECTION_STRING)
db = client["IT4063E"]  
collection = db["Amazon"]  


scala_version = '2.12'
spark_version = '3.3.1'
packages = [
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
    'org.apache.kafka:kafka-clients:3.5.0',
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1"
]


spark = SparkSession.builder \
    .appName("Spark SQL") \
    .master('local[*]') \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.mongodb.connection.uri", CONNECTION_STRING) \
    .config("spark.local.dir", "./spark_temp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


cursor = collection.find(batch_size=8)  # Trả về một cursor

for document in cursor:
    record_found = True  
    df = spark.createDataFrame([document], schema=AMAZON_SCHEMA)
    df.show()
    # Xuất ra console hoặc thực hiện các hành động khác
    query = df.selectExpr("CAST(review_id_indexed AS STRING)", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
        .option("topic", AMAZON_TOPIC) \
        .save()