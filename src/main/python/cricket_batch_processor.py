from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, min, count, window, col
import time

def create_spark_session():
    return SparkSession.builder \
        .appName("CricketBatchProcessor") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

def process_batch_data(spark):
    # Database connection properties
    jdbc_url = "jdbc:postgresql://localhost:5432/cricket_db"
    properties = {
        "user": "postgres",
        "password": "cricket123",  # Updated to match bowling processor
        "driver": "org.postgresql.Driver"
    }

    # Process batting statistics
    start_time = time.time()
    
    batting_df = spark.read \
        .jdbc(url=jdbc_url, table="batting_stats", properties=properties)
    
    batting_analysis = batting_df \
        .groupBy("player_name") \
        .agg(
            sum("total_runs").alias("career_runs"),
            avg("strike_rate").alias("avg_strike_rate"),
            sum("balls_faced").alias("total_balls_faced"),
            max("total_runs").alias("highest_score"),
            min("strike_rate").alias("lowest_strike_rate"),
            count("*").alias("innings_count")
        )

    # Add window-based analysis
    windowed_analysis = batting_df \
        .groupBy(
            window("window_start", "30 minutes"),
            "player_name"
        ) \
        .agg(
            sum("total_runs").alias("runs_in_window"),
            avg("strike_rate").alias("window_strike_rate")
        ) \
        .select(
            "player_name",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "runs_in_window",
            "window_strike_rate"
        )

    # Save all analyses
    windowed_analysis.write \
        .jdbc(url=jdbc_url, table="windowed_batting_analysis", mode="overwrite", properties=properties)

    # Process bowling statistics
    bowling_df = spark.read \
        .jdbc(url=jdbc_url, table="bowling_stats", properties=properties)
    
    bowling_analysis = bowling_df \
        .groupBy("bowler_name") \
        .agg(
            sum("total_wickets").alias("career_wickets"),
            avg("economy_rate").alias("avg_economy"),
            sum("total_runs_conceded").alias("total_runs_conceded"),
            max("total_wickets").alias("best_bowling_figures"),
            min("economy_rate").alias("best_economy"),
            count("*").alias("spells_bowled")
        )

    end_time = time.time()
    processing_time = end_time - start_time

    # Save batch processing results
    batting_analysis.write \
        .jdbc(url=jdbc_url, table="batch_batting_analysis", mode="overwrite", properties=properties)
    
    bowling_analysis.write \
        .jdbc(url=jdbc_url, table="batch_bowling_analysis", mode="overwrite", properties=properties)

    return processing_time

if __name__ == "__main__":
    spark = create_spark_session()
    processing_time = process_batch_data(spark)
    print(f"Batch processing completed in {processing_time:.2f} seconds")
