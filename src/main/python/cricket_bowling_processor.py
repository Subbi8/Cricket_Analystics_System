from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("CricketBowlingProcessor") \
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
            "bowler_name",
            "total_wickets",
            "total_runs_conceded",
            "economy_rate"
        )
        
        # Create temporary view
        processed_df.createOrReplaceTempView("updates")
        
        processed_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/cricket_db") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "bowling_stats") \
            .option("user", "postgres") \
            .option("password", "cricket123") \
            .option("batchsize", 1000) \
            .option("isolationLevel", "NONE") \
            .mode("append") \
            .save()
        print(f"Successfully wrote batch {batch_id} to PostgreSQL")

def process_bowling_stream(spark):
    # Create checkpoint directory using Linux path
    checkpoint_location = "/tmp/bowling_checkpoint"
    if not os.path.exists(checkpoint_location):
        os.makedirs(checkpoint_location)

    bowling_schema = StructType([
        StructField("bowler_name", StringType()),
        StructField("wickets", IntegerType()),
        StructField("runs_conceded", IntegerType()),
        StructField("match_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("event_type", StringType())
    ])

    try:
        bowling_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "cricket-bowling-topic") \
            .option("startingOffsets", "latest") \
            .load()

        bowling_stats = bowling_df \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), bowling_schema).alias("data")) \
            .select("data.*") \
            .withWatermark("timestamp", "15 minutes") \
            .groupBy(
                window("timestamp", "15 minutes"),
                "bowler_name"
            ) \
            .agg(
                sum("wickets").alias("total_wickets"),
                sum("runs_conceded").alias("total_runs_conceded"),
                (sum("runs_conceded") / 2.5).alias("economy_rate")
            )

        query = bowling_stats.writeStream \
            .foreachBatch(write_batch_to_postgres) \
            .outputMode("update") \
            .option("checkpointLocation", checkpoint_location) \
            .trigger(processingTime='5 seconds') \
            .start()

        return query

    except Exception as e:
        print(f"Error in stream processing: {str(e)}")
        raise e

if __name__ == "__main__":
    try:
        spark = create_spark_session()
        query = process_bowling_stream(spark)
        print("Bowling stream processing started...")
        query.awaitTermination()
    except Exception as e:
        print(f"Application error: {str(e)}")
