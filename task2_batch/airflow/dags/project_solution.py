from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'dmzhuk',
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'dmzhuk_datalake_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['goit', 'de', 'final_project'],
) as dag:
    base_path = "/opt/airflow/task2_batch"
    scripts_dir = f"{base_path}/scripts"
    jar_path = f"{base_path}/jars/mysql-connector-j-8.0.32.jar"

    common_spark_config = {
        'conn_id': 'spark_default',
        'jars': jar_path,
        'verbose': True
    }

    # 1. Landing to Bronze
    task_landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application=f'{scripts_dir}/landing_to_bronze.py',
        name='spark_landing_to_bronze',
        **common_spark_config
    )

    # 2. Bronze to Silver
    task_bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application=f'{scripts_dir}/bronze_to_silver.py',
        name='spark_bronze_to_silver',
        **common_spark_config
    )

    # 3. Silver to Gold
    task_silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application=f'{scripts_dir}/silver_to_gold.py',
        name='spark_silver_to_gold',
        **common_spark_config
    )

    task_landing_to_bronze >> task_bronze_to_silver >> task_silver_to_gold
