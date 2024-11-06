import os
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, Normalizer
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
from config import CONNECTION_STRING, MASTER, ES_NODES
from schemas import AMAZON_SCHEMA


print(findspark.init())
os.environ["PYSPARK_PYTHON"] = "python"
print("SPARK_HOME:", os.environ.get('SPARK_HOME'))
print("PYTHONPATH:", os.environ.get('PYTHONPATH'))
print("HADOOP_HOME:", os.environ.get('HADOOP_HOME'))
print("JAVA_HOME:", os.environ.get('JAVA_HOME'))
print(f"PySpark version: {pyspark.__version__}")

packages = [
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
]

spark = SparkSession.builder \
    .appName("Matrix Factorization Example") \
    .master('local[*]') \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.mongodb.connection.uri", CONNECTION_STRING) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir('checkpoint/')

df = spark.read.format("mongodb") \
                .option("database", "IT4063E") \
                .option("collection", "Amazon") \
                .load(schema=AMAZON_SCHEMA)


df = df.select("product_id_indexed", "about_product")

# Xây dựng các stages trong pipeline
# Bước 1: Tokenize phần mô tả sản phẩm
tokenizer = Tokenizer(inputCol="about_product", outputCol="words")

# Bước 2: Tính toán TF
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)

# Bước 3: Tính toán IDF
idf = IDF(inputCol="rawFeatures", outputCol="features")

# Bước 4: Chuẩn hóa vector đặc trưng để tính cosine similarity
normalizer = Normalizer(inputCol="features", outputCol="normFeatures")

# Định nghĩa pipeline
pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, normalizer])

# Fit và transform dữ liệu qua pipeline
model = pipeline.fit(df)
data = model.transform(df)

# Tính cosine similarity với sản phẩm có productId = 1
product1 = data.where(col("productId") == 1).select("normFeatures").first()[0]

# Hàm UDF để tính độ tương đồng cosine giữa các sản phẩm
def cosine_similarity(features):
    return float(features.dot(Vectors.dense(product1.toArray())))

cosine_similarity_udf = udf(cosine_similarity, DoubleType())

# Áp dụng hàm cosine similarity và sắp xếp theo độ tương đồng
data = data.withColumn("similarity", cosine_similarity_udf(col("normFeatures")))
result = data.select("productId", "similarity").orderBy(col("similarity").desc())

# Hiển thị kết quả
result.show()

# Dừng SparkSession
spark.stop()