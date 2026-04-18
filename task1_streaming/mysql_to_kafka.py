import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F

# 1. Завантаження секретів
load_dotenv()

# 2. Отримання налаштувань
jdbc_url = os.getenv("REMOTE_DB_URL")
jdbc_user = os.getenv("REMOTE_DB_USER")
jdbc_password = os.getenv("REMOTE_DB_PASS")
kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or "localhost:9092"

if not jdbc_url:
    print("ПОМИЛКА: Не знайдено REMOTE_DB_URL у файлі .env")
    exit(1)

# 3. Ініціалізація Spark
spark = SparkSession.builder \
    .appName("MySQLToKafka_Producer") \
    .getOrCreate()

# Етап 3.1: Зчитування
print(f">>> Зчитуємо дані з віддаленого MySQL: {jdbc_url}")
df = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="athlete_event_results",
    user=jdbc_user,
    password=jdbc_password
).load()

# Етап 3.2: Перетворення в JSON
print(">>> Перетворюємо дані в JSON формат...")
kafka_json_df = df.select(F.to_json(F.struct("*")).alias("value"))

# Етап 3.3: Запис у Kafka
print(f">>> Відправляємо дані в Kafka ({kafka_server})...")
kafka_json_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("topic", "athlete_event_results") \
    .save()

print("Етап 3 завершено - дані в Kafka")
spark.stop()