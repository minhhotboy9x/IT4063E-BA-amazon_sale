import os

import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import explode, col, from_json, current_timestamp, expr, udf
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

ES_RESOURCE = "amazon"

def write_to_elasticsearch(df, epoch_id):
    df.show()
    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", ES_NODES) \
        .option("es.resource", ES_RESOURCE) \
        .option("es.mapping.id", "id") \
        .option("es.write.operation", "upsert") \
        .option("es.index.auto.create", "true") \
        .option("es.nodes.wan.only", "true") \
        .mode("append") \
        .save(ES_RESOURCE)

client = MongoClient(CONNECTION_STRING)
db = client["IT4063E"]  
collection = db["Amazon"]  

def get_data_from_mongo(batch_size):
    cursor = collection.find(batch_size=batch_size)
    for document in cursor:
        yield document  


scala_version = '2.12'
spark_version = '3.3.1'
packages = [
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
    'org.apache.kafka:kafka-clients:3.5.0',
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1",
    "org.elasticsearch:elasticsearch-spark-30_2.12:7.17.16"
]


spark = SparkSession.builder \
    .appName("Spark SQL") \
    .master('local[*]') \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.mongodb.connection.uri", CONNECTION_STRING) \
    .config("spark.local.dir", "./spark_temp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
    .option("subscribe", AMAZON_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

df = df.selectExpr("CAST(value AS STRING)") \
                .select(from_json("value", AMAZON_SCHEMA).alias('review'))

df = df.select('review.*')
df = df.withColumn("id", current_timestamp())

query = df.writeStream \
    .foreachBatch(write_to_elasticsearch) \
    .outputMode("append") \
    .start()

query.awaitTermination()
