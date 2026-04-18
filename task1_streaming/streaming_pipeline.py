import os
from dotenv import load_dotenv
from pyspark.sql.functions import col, isnan
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

load_dotenv()

# Налаштування для Spark сесії
jar_path = "/home/ubuntu/projects/goit-de-fp/mysql-connector-j-8.0.32.jar"

remote_mysql_config = {
    "url": os.getenv("REMOTE_DB_URL"),
    "user": os.getenv("REMOTE_DB_USER"),
    "password": os.getenv("REMOTE_DB_PASS"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Локальний конфіг для запису результатів
local_mysql_config = {
    "url": os.getenv("LOCAL_DB_URL"),
    "user": os.getenv("LOCAL_DB_USER"),
    "password": os.getenv("LOCAL_DB_PASS"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

spark = SparkSession.builder \
    .appName("OlympicStreamingEnrichment") \
    .config("spark.jars", jar_path) \
    .getOrCreate()

# 1. Читаємо статичні дані
athlete_bio_df = spark.read.format("jdbc").options(**remote_mysql_config) \
    .option("dbtable", "athlete_bio").load()

# Етап 1, 2: фільтрація NaN/Null
athlete_bio_df_clean = athlete_bio_df.filter(
    col("height").isNotNull() & (~isnan(col("height"))) &
    col("weight").isNotNull() & (~isnan(col("weight")))
).select("athlete_id", "sex", "country_noc", "height", "weight")

# 3. Результати з Kafka (Stream)
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS")) \
    .option("subscribe", "athlete_event_results") \
    .option("startingOffsets", "earliest") \
    .load()

json_schema = StructType([
    StructField("athlete_id", IntegerType()),
    StructField("sport", StringType()),
    StructField("medal", StringType())
])

results_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json("value", json_schema).alias("data")) \
    .select("data.*")

# Етап 4: Join за athlete_id (без дублікатів)
enriched_stream = results_stream.join(athlete_bio_df_clean, "athlete_id")

# Етап 5: Трансформація та агрегація
aggregated_stats = enriched_stream.groupBy(
    "sport", "medal", "sex", "country_noc"
).agg(
    F.avg("height").alias("avg_height"),
    F.avg("weight").alias("avg_weight"),
    F.current_timestamp().alias("calculation_timestamp")
)

# Етап 6: forEachBatch задля Fan-Out запису
def foreach_batch_function(batch_df, batch_id):
    # Очистка від NaN в агрегатах
    clean_batch_df = batch_df.filter(
        (~isnan(col("avg_height"))) & (~isnan(col("avg_weight")))
    )
    
    if clean_batch_df.count() > 0:
        # a) запис у вихідний Kafka-топік
        clean_batch_df.select(F.to_json(F.struct("*")).alias("value")).write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS")) \
            .option("topic", "athlete_stats_enriched") \
            .save()
        
        # b) запис у локальну MySQL
        clean_batch_df.write.format("jdbc") \
                .options(**local_mysql_config) \
                .option("dbtable", "enriched_stats_results") \
                .mode("append") \
                .save()

# Запуск стріму
query = aggregated_stats.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .start()

query.awaitTermination()