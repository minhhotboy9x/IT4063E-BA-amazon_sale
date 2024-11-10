from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, IntegerType

AMAZON_SCHEMA = StructType([
    StructField("product_name", StringType(), True),
    StructField("discounted_price", FloatType(), True),
    StructField("actual_price", FloatType(), True),
    StructField("discount_percentage", FloatType(), True),
    StructField("rating", FloatType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("about_product", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("review_title", StringType(), True),
    StructField("review_content", StringType(), True),
    StructField("img_link", StringType(), True),
    StructField("product_link", StringType(), True),
    StructField("product_id_indexed", IntegerType(), True),
    StructField("user_id_indexed", IntegerType(), True),
    StructField("review_id_indexed", IntegerType(), True),
    StructField("category_list", ArrayType(StringType(), True))
])

RAW_AMAZON_SCHEMA = StructType([
    StructField("product_name", StringType(), True),
    StructField("discounted_price", StringType(), True),
    StructField("actual_price", StringType(), True),
    StructField("discount_percentage", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("rating_count", StringType(), True),
    StructField("about_product", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("review_title", StringType(), True),
    StructField("review_content", StringType(), True),
    StructField("img_link", StringType(), True),
    StructField("product_link", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("review_id", StringType(), True),
    StructField("category", StringType(), True)
])

PREDICTED_AMAZON_SCHEMA = StructType([
    StructField("product_name", StringType(), True),
    StructField("discounted_price", FloatType(), True),
    StructField("actual_price", FloatType(), True),
    StructField("discount_percentage", FloatType(), True),
    StructField("rating", FloatType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("about_product", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("review_title", StringType(), True),
    StructField("review_content", StringType(), True),
    StructField("img_link", StringType(), True),
    StructField("product_link", StringType(), True),
    StructField("product_id_indexed", IntegerType(), True),
    StructField("user_id_indexed", IntegerType(), True),
    StructField("review_id_indexed", IntegerType(), True),
    StructField("category_list", ArrayType(StringType(), True)),
    StructField("optimized_price", FloatType(), True)
])