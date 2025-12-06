from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# 1. Initialize Spark with Delta Lake & S3 support
spark = SparkSession.builder \
    .appName("StockDataStreaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define Schema (Must match what producer.py sends)
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("volume", LongType(), True)
])

# 3. Read Stream from Kafka
print("Reading from Kafka...")
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Transform Data (Parse JSON)
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("processing_time", current_timestamp())

# 5. Write Stream to MinIO (Delta Lake format)
print("Writing to MinIO...")
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://datalake/checkpoints/stocks") \
    .option("path", "s3a://datalake/bronze/stocks") \
    .start()

query.awaitTermination()