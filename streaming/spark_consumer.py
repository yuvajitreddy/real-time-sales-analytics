import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, from_json, window
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
    .appName("SalesAnalytics") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("order_id",  StringType()) \
    .add("product",   StringType()) \
    .add("region",    StringType()) \
    .add("amount",    DoubleType()) \
    .add("timestamp", StringType())

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")) \
    .option("subscribe", os.getenv("KAFKA_TOPIC", "sales-events")) \
    .option("startingOffsets", "latest") \
    .load()

sales_df = raw \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))

agg = sales_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window("event_time", "1 minute"),
        "region",
        "product"
    ).agg(
        _sum("amount").alias("total_sales"),
        count("order_id").alias("order_count")
    )

def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("region"),
        col("product"),
        col("total_sales"),
        col("order_count")
    ).write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}") \
        .option("dbtable", "sales_aggregated") \
        .option("user", os.getenv("POSTGRES_USER")) \
        .option("password", os.getenv("POSTGRES_PASSWORD")) \
        .mode("append") \
        .save()
    print(f"Batch {batch_id} written to PostgreSQL")

def write_to_s3(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    batch_df.write \
        .mode("append") \
        .parquet(f"s3a://{os.getenv('S3_BUCKET')}/raw-sales/batch-{batch_id}")
    print(f"Batch {batch_id} written to S3")

print("Starting Spark Streaming...")

postgres_query = agg.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()

raw_query = sales_df.writeStream \
    .foreachBatch(write_to_s3) \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()