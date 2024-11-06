import os

import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from schemas import AMAZON_SCHEMA
from config import CONNECTION_STRING, MASTER, AMAZON_TOPIC, KAFKA_BROKER_PRODUCER
from pymongo import MongoClient
from kafka import KafkaProducer
from pprint import pprint
import time

print(findspark.init())
os.environ['PYSPARK_PYTHON'] = "python"
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
    # "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
    # 'org.apache.kafka:kafka-clients:3.5.0',
    # "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1"
]


spark = SparkSession.builder \
    .appName("Producer") \
    .master(MASTER) \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.mongodb.connection.uri", CONNECTION_STRING) \
    .config("spark.driver.host","192.168.137.1") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.local.dir", "spark_temp") \
    .config("spark.cores.max", "2") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


cursor = collection.find(batch_size=8) 
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_PRODUCER)
print("Connected to Kafka")
for document in cursor:
    if not document:
        break
    df = spark.createDataFrame([document], schema=AMAZON_SCHEMA)
    # df.show()
    pprint(document)
    df_json = df.selectExpr("CAST(review_id_indexed AS STRING)", "to_json(struct(*)) AS value")
    for row in df_json.collect():
        producer.send(
            topic=AMAZON_TOPIC,
            key=str(row['review_id_indexed']).encode('utf-8'),
            value=row['value'].encode('utf-8')
        )
        producer.flush()
    time.sleep(1)
    del df

client.close()