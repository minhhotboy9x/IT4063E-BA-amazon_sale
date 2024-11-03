from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import count, when, col
from pyspark.sql import Row
from config import CONNECTION_STRING, MASTER, ES_NODES
from schemas import AMAZON_SCHEMA

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

df = spark.read.format("mongodb") \
                .option("database", "IT4063E") \
                .option("collection", "Amazon") \
                .load(schema=AMAZON_SCHEMA)


df = df.select("user_id_indexed", "product_id_indexed", "rating")
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()
# df.show()

als = ALS(rank=16,
          maxIter=200, 
          regParam=0.01, 
          userCol="user_id_indexed", 
          itemCol="product_id_indexed", 
          ratingCol="rating", 
          coldStartStrategy="drop")

(training, test) = df.randomSplit([0.8, 0.2])

model = als.fit(training)
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# Đường dẫn để lưu mô hình
model_path = "MF_model/"

# Lưu mô hình
model.write().overwrite().save(model_path)