import os

import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from schemas import AMAZON_SCHEMA
from config import CONNECTION_STRING, MASTER, ES_NODES, AMAZON_TOPIC, KAFKA_BROKER_CONSUMER, LOCAL_HOST
from kafka import KafkaConsumer
import json

print(findspark.init())
os.environ["PYSPARK_PYTHON"] = "python"
print("SPARK_HOME:", os.environ.get('SPARK_HOME'))
print("PYTHONPATH:", os.environ.get('PYTHONPATH'))
print("HADOOP_HOME:", os.environ.get('HADOOP_HOME'))
print("JAVA_HOME:", os.environ.get('JAVA_HOME'))
print(f"PySpark version: {pyspark.__version__}")

ES_RESOURCE = "amazon"

scala_version = '2.12'
spark_version = '3.3.1'
packages = [
    # 'org.apache.kafka:kafka-clients:3.5.0',
    # "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1",
    "org.elasticsearch:elasticsearch-spark-30_2.12:7.17.16"
]

spark = SparkSession.builder \
    .appName("Consumer") \
    .master(MASTER) \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.mongodb.connection.uri", CONNECTION_STRING) \
    .config("spark.driver.host", LOCAL_HOST) \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.local.dir", "spark_temp") \
    .config("spark.cores.max", "2") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()


def get_kafka_data():
    """Generator to get Kafka messages."""
    consumer = KafkaConsumer(
        'amazon',
        bootstrap_servers=KAFKA_BROKER_CONSUMER,
        auto_offset_reset='latest'
    )
    print("Connected to Kafka", consumer.bootstrap_connected())
    for message in consumer:
        if message.value:
            yield json.loads(message.value.decode('utf-8'))

def write_to_elasticsearch(df, id):
    """Process and write data to Elasticsearch."""
    df.select('review_id_indexed', 'product_id_indexed', 
              'user_id_indexed', 'rating').show()
    df.write \
        .format("es") \
        .option("es.nodes", ES_NODES) \
        .option("es.resource", ES_RESOURCE) \
        .option("es.nodes.wan.only", "true") \
        .option("es.mapping.id", "review_id_indexed") \
        .option("es.write.operation", "upsert") \
        .mode("append") \
        .save()

def stream_data():
    print("Streaming data from Kafka to Elasticsearch...")
    for message in get_kafka_data():
        df = spark.createDataFrame([message], schema=AMAZON_SCHEMA)
        write_to_elasticsearch(df, 0)

stream_data()


# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER_CONSUMER) \
#     .option("subscribe", AMAZON_TOPIC) \
#     .option("startingOffsets", "latest") \
#     .load()

# df = df.selectExpr("CAST(value AS STRING)") \
#                 .select(from_json("value", AMAZON_SCHEMA).alias('amazon'))
# df = df.select('amazon.*')
# df = df.withColumnRenamed('id', 'review_id_indexed')

# query = df.writeStream \
#     .outputMode("append") \
#     .foreachBatch(write_to_elasticsearch) \
#     .start()

# query.awaitTermination()