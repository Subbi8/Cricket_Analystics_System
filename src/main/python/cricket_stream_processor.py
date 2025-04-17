from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("CricketStreamProcessor") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,"
                "org.postgresql:postgresql:42.2.18") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

def write_batch_to_postgres(batch_df, batch_id):
    if not batch_df.isEmpty():
        processed_df = batch_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "player_name",
            "total_runs",
            "balls_faced",
            "strike_rate"
        )
        
        # Create temporary view
        processed_df.createOrReplaceTempView("updates")
        
        # Use PostgreSQL upsert syntax
        processed_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/cricket_db") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "batting_stats") \
            .option("user", "postgres") \
            .option("password", "cricket123") \
            .option("batchsize", 1000) \
            .option("isolationLevel", "NONE") \
            .mode("overwrite") \
            .save()
        print(f"Successfully wrote batch {batch_id} to PostgreSQL")

def process_batting_stream(spark):
    # Create checkpoint directory
    checkpoint_location = "/tmp/checkpoint"
    if not os.path.exists(checkpoint_location):
        os.makedirs(checkpoint_location)

    batting_schema = StructType([
        StructField("player_name", StringType()),
        StructField("runs", IntegerType()),
        StructField("balls", IntegerType()),
        StructField("match_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("event_type", StringType())
    ])

    batting_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "cricket-batting-topic") \
        .option("startingOffsets", "latest") \
        .load()

    batting_stats = batting_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), batting_schema).alias("data")) \
        .select("data.*") \
        .withWatermark("timestamp", "15 minutes") \
        .groupBy(
            window("timestamp", "15 minutes"),
            "player_name"
        ) \
        .agg(
            sum("runs").alias("total_runs"),
            sum("balls").alias("balls_faced"),
            (sum("runs") * 100.0 / sum("balls")).alias("strike_rate")
        )

    query = batting_stats.writeStream \
        .foreachBatch(write_batch_to_postgres) \
        .outputMode("update") \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime='5 seconds') \
        .start()

    return query

if __name__ == "__main__":
    spark = create_spark_session()
    query = process_batting_stream(spark)
    print("Stream processing started...")
    query.awaitTermination()
