import os
from pyspark.sql import SparkSession
from config import MASTER, CONNECTION_STRING
from schemas import AMAZON_SCHEMA
from pymongo import MongoClient

os.environ['PYSPARK_PYTHON'] = "python"
# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("Simple Application") \
    .master(MASTER) \
    .config("spark.local.dir", "./spark_temp") \
    .config("spark.cores.max", "1") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

# Tạo DataFrame mẫu
data = [("Alice", 1), ("Bob", 2)]
df = spark.createDataFrame(data, ["name", "id"])

# Hiển thị DataFrame
df.show()

client = MongoClient(CONNECTION_STRING)
db = client["IT4063E"]  
collection = db["Amazon"]  
cursor = collection.find(batch_size=8) 
for document in cursor:
    if not document:
        break
    df = spark.createDataFrame([document], schema=AMAZON_SCHEMA)
    # df.show()
    df_json = df.selectExpr("CAST(review_id_indexed AS STRING)", "to_json(struct(*)) AS value")

# Dừng Spark session
spark.stop()
