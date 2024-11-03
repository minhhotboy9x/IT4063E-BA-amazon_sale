import os

import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.sql.types import FloatType, StringType, DoubleType, ArrayType
from pyspark.sql.functions import udf
import re
from config import CONNECTION_STRING, MASTER
from schemas import AMAZON_SCHEMA, RAW_AMAZON_SCHEMA


print(findspark.init())
os.environ["PYSPARK_PYTHON"] = "python"
print("SPARK_HOME:", os.environ.get('SPARK_HOME'))
print("PYTHONPATH:", os.environ.get('PYTHONPATH'))
print("HADOOP_HOME:", os.environ.get('HADOOP_HOME'))
print("JAVA_HOME:", os.environ.get('JAVA_HOME'))
print(f"PySpark version: {pyspark.__version__}")


scala_version = '2.12'
spark_version = '3.3.1'
packages = [
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
]


def convert_to_list(category_str):
    return category_str.split('|')

convert_to_list_udf = udf(lambda x: convert_to_list(x), ArrayType(StringType(), True))


spark = SparkSession.builder \
    .appName("Spark SQL") \
    .master("local[*]") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.mongodb.write.connection.uri", CONNECTION_STRING) \
    .config("spark.local.dir", "./spark_temp") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# local_dir = spark.conf.get("spark.local.dir")
df = spark.read.csv("data/amazon.csv", header=True, inferSchema=True, escape='"' )
df = df.drop('img_link', 'product_link')


# reformat data: category, actual_price, discounted_price, discount_percentage, rating, rating_count
df = df.withColumn("category_list", convert_to_list_udf(col('category'))).drop("category")

# # result = df.filter((df['discount_percentage'] == "49%") & (df['rating_count'] == "7,429"))
# # result.show()

# Lọc số từ cột 'actual_price'
df = df.withColumn(
    "actual_price",
    F.regexp_replace(F.col("actual_price"), "[^0-9.]", "").cast("float")
)


# # Lọc số từ cột 'discounted_price'
df = df.withColumn(
    'discounted_price',
    F.regexp_replace(F.col("discounted_price"), "[^0-9.]", "").cast("float")
)

# # Lọc số từ cột 'discount_percentage' (nếu là số với ký hiệu %)
df = df.withColumn(
    'discount_percentage',
    F.regexp_replace(F.col("discount_percentage"), "[^0-9.]", "").cast("float")
)

df = df.withColumn(
    'rating',
    F.regexp_replace(F.col("rating"), "[^0-9.]", "").cast("float")
)

df = df.withColumn(
    'rating_count',
    F.regexp_replace(F.col("rating_count"), "[^0-9]", "").cast("int")
)

# drop nan and duplicates ----------------------------------------------
print(df.count())
df = df.dropna(how="any")
print(df.count())
df = df.dropDuplicates(['review_id'])
print(df.count())

# indexing product_id and user_id ----------------------------------------------
indexer_product = StringIndexer(inputCol="product_id", outputCol="product_id_indexed")
indexer_user = StringIndexer(inputCol="user_id", outputCol="user_id_indexed")
indexer_review = StringIndexer(inputCol="review_id", outputCol="review_id_indexed")

pipeline = Pipeline(stages=[indexer_product, indexer_user, indexer_review])

model = pipeline.fit(df)
df =  model.transform(df)
df = df \
    .withColumn("product_id_indexed", col("product_id_indexed").cast("int")) \
    .withColumn("user_id_indexed", col("user_id_indexed").cast("int")) \
    .withColumn("review_id_indexed", col("review_id_indexed").cast("int"))
df = df.drop("product_id", "user_id", "review_id")


df.write.format("mongodb") \
            .option("database", "IT4063E") \
            .option("collection", "Amazon") \
            .mode("overwrite") \
            .save()

