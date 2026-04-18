import os
import requests
from pyspark.sql import SparkSession

def download_data(table_name, target_dir):
    url = "https://ftp.goit.study/neoversity/"
    local_file_path = os.path.join(target_dir, f"{table_name}.csv")
    downloading_url = url + f"{table_name}.csv"
    
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        with open(local_file_path, 'wb') as file:
            file.write(response.content)
        print(f"File {table_name} downloaded successfully to {local_file_path}")
        return local_file_path
    else:
        print(f"Failed to download {table_name}. Status: {response.status_code}")
        return None

def run():
    base_dir = "/opt/airflow/task2_batch"
    scripts_dir = os.path.join(base_dir, "scripts")
    
    if not os.path.exists(scripts_dir):
        os.makedirs(scripts_dir)

    spark = SparkSession.builder \
        .appName("LandingToBronze") \
        .getOrCreate()
    
    tables = ["athlete_bio", "athlete_event_results"]

    for folder in ["bronze", "silver", "gold"]:
        path = os.path.join(base_dir, folder)
        if not os.path.exists(path):
            os.makedirs(path)

    for table in tables:
        csv_path = download_data(table, scripts_dir)
        if csv_path and os.path.exists(csv_path):
            
            # Читання csv
            df = spark.read.csv(csv_path, header=True, inferSchema=True)
            
            # Запис у parquet
            output_path = os.path.join(base_dir, "bronze", table)
            df.write.mode("overwrite").parquet(output_path)
            
            print(f"--- BRONZE DATA PREVIEW: {table} ---")
            df.show(5)
        else:
            print(f"Skipping table {table} due to download error")

    spark.stop()

if __name__ == "__main__":
    run()