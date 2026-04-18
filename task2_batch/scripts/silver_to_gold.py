import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, current_timestamp

def run():
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .getOrCreate()

    base_dir = "/opt/airflow/task2_batch"

    # Завантаження даних
    print("Reading silver tables...")
    bio_df = spark.read.parquet(os.path.join(base_dir, "silver", "athlete_bio"))
    results_df = spark.read.parquet(os.path.join(base_dir, "silver", "athlete_event_results"))

    # Join за athlete_id
    joined_df = bio_df.join(results_df.drop("country_noc"), "athlete_id")

    # Конвертація та фільтрація
    gold_df = joined_df.withColumn("weight", col("weight").cast("float")) \
                       .withColumn("height", col("height").cast("float")) \
                       .filter(col("weight").isNotNull() & col("height").isNotNull())

    # Агрегація
    print("Calculating aggregations...")
    final_gold = gold_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            round(avg("weight"), 2).alias("avg_weight"),
            round(avg("height"), 2).alias("avg_height")
        ) \
        .withColumn("timestamp", current_timestamp())

    # Запис результату
    output_path = os.path.join(base_dir, "gold", "avg_stats")
    final_gold.write.mode("overwrite").parquet(output_path)
    
    # Вивід для логів Airflow
    print("--- FINAL GOLD AGGREGATION TABLE: avg_stats ---")
    final_gold.show(10)
    print(f"✅ Success! Gold table created at {output_path}")
    
    spark.stop()

if __name__ == "__main__":
    run()