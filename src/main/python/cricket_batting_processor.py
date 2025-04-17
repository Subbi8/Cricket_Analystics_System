from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import yaml
import os

def create_spark_session():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'resources', 'config.yaml')
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    return SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,"
                "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

def process_batting_stream(spark, config):
    # Update schema to match SQL table
    batting_schema = StructType([
        StructField("player_name", StringType()),
        StructField("runs", IntegerType()),
        StructField("balls_faced", IntegerType()),
        StructField("timestamp", TimestampType())
    ])

    batting_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config['kafka']['bootstrap_servers']) \
        .option("subscribe", config['kafka']['topics']['batting']) \
        .option("startingOffsets", "latest") \
        .load()

    # Add null value handling and data validation
    # Simplify parsed_df to match SQL schema
    parsed_df = batting_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), batting_schema).alias("data")) \
        .select("data.*") \
        .filter(col("player_name").isNotNull() & 
                col("runs").isNotNull() & 
                col("balls_faced").isNotNull()) \
        .withColumn("runs", when(col("runs") < 0, 0).otherwise(col("runs"))) \
        .withColumn("balls_faced", when(col("balls_faced") <= 0, 1).otherwise(col("balls_faced")))

    # Update aggregations to match SQL schema exactly
    batting_stats = parsed_df \
        .withWatermark("timestamp", config['streaming']['watermark']) \
        .groupBy(
            window("timestamp", config['streaming']['window_duration']),
            "player_name"
        ) \
        .agg(
            coalesce(sum("runs"), lit(0)).alias("total_runs"),
            coalesce(sum("balls_faced"), lit(0)).alias("balls_faced"),
            (coalesce(sum("runs"), lit(0)) * 100.0 / 
             coalesce(sum("balls_faced"), lit(1))).cast("decimal(10,2)").alias("strike_rate")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("player_name"),
            col("total_runs"),
            col("balls_faced"),
            col("strike_rate")
        )

    # Enhanced error handling for database writes
    def write_batch_to_postgres(batch_df, batch_id):
        try:
            if not batch_df.isEmpty():
                batch_df.write \
                    .format("jdbc") \
                    .option("url", f"jdbc:postgresql://{config['postgresql']['host']}:{config['postgresql']['port']}/{config['postgresql']['database']}") \
                    .option("driver", "org.postgresql.Driver") \
                    .option("dbtable", "batting_stats") \
                    .option("user", config['postgresql']['user']) \
                    .option("password", config['postgresql']['password']) \
                    .mode("append") \
                    .save()
        except Exception as e:
            print(f"Error writing batch {batch_id} to database: {str(e)}")
            # Log error but don't fail the stream
            pass

    query = batting_stats.writeStream \
        .foreachBatch(write_batch_to_postgres) \
        .outputMode("update") \
        .start()

    return query

if __name__ == "__main__":
    try:
        config_path = os.path.join(os.path.dirname(__file__), '..', 'resources', 'config.yaml')
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        spark = create_spark_session()
        query = process_batting_stream(spark, config)
        print("Batting stream processing started...")
        query.awaitTermination()
    except Exception as e:
        print(f"Application error: {str(e)}")
