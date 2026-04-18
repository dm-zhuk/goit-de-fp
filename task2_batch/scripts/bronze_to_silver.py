import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.types import StringType

def run():
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .getOrCreate()

    base_dir = "/opt/airflow/task2_batch"
    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        df = spark.read.parquet(os.path.join(base_dir, "bronze", table))
        text_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        for col_name in text_cols:
            df = df.withColumn(col_name, regexp_replace(col(col_name), r'[^a-zA-Z0-9,.\\"\']', ''))
            df = df.withColumn(col_name, trim(col(col_name)))
        
        df = df.dropDuplicates()
        
        output_path = os.path.join(base_dir, "silver", table)
        df.write.mode("overwrite").parquet(output_path)
        
        print(f"--- SILVER DATA PREVIEW (CLEANED): {table} ---")
        df.show(5, truncate=False)

    spark.stop()

if __name__ == "__main__":
    run()