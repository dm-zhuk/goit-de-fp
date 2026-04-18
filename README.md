# 🏅 Data Engineering Final Project: Streaming & Batch ETL

[![Spark](https://img.shields.io/badge/Apache_Spark-3.5.1-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Airflow](https://img.shields.io/badge/Apache_Airflow-2.8.1-017CEE?style=flat&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5.0-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)

Цей проект демонструє побудову комплексної екосистеми обробки даних: від **Real-time Streaming** до **Multi-layer Batch Datalake**.

---

## Основні модулі

### 1️⃣ Real-time Data Pipeline (Task 1)
Потокова обробка сенсорних даних з використанням Kafka та Spark Structured Streaming.
- **Джерело:** MySQL (через Python producer).
- **Обробка:** Ковзаючі вікна (Sliding Windows) та агрегація.
- **Output:** Вивід результатів у консоль у реальному часі.

> ****
> *скриншот терміналу з таблицею результатів:*

![Job1_report](/screenshots/Job1_report.png)

---

### 2️⃣ Medallion Data Lake (Task 2)
ETL-пайплайн, оркестрований Airflow, для обробки історичних даних про Олімпійські ігри.

| Рівень | Опис | Технології |
| :--- | :--- | :--- |
| **Landing** | HTTP Download (FTP GoIT) | Python `requests` |
| **Bronze** | Raw Data ingestion | Spark Parquet |
| **Silver** | Data Cleaning (Regex, De-duplication) | PySpark SQL |
| **Gold** | Joins & Analytics (Final Stats) | PySpark Aggregations |

---

## Оптимізація під Legacy Hardware
Проект успішно розгорнуто на **MacBook Pro Mid-2012** (8GB RAM) завдяки наступним оптимізаціям:
- **Native Spark Functions:** Заміна повільних Python UDF на вбудовані `regexp_replace`.
- **Docker Resource Tuning:** Обмеження пам'яті для Spark Workers (1GB).
- **Unified Path Mapping:** Синхронізація волюмів між Airflow та Spark для уникнення затримок I/O.

---

## Візуалізація результатів

### Airflow Orchestration
![Airflow Success Path](task2_batch/screenshots/airflow_dag_success.png)
*(Заміни шлях на свій скриншот)*

### Spark Job Execution
![Spark Master Status](task2_batch/screenshots/spark_master_finished.png)
*(Заміни шлях на свій скриншот)*

---

## Швидкий старт (Deployment)

```bash
# 1. Клонування
git clone [https://github.com/dm-zhuk/goit-de-fp.git](https://github.com/dm-zhuk/goit-de-fp.git) && cd task2_batch

# 2. Запуск інфраструктури
docker compose up -d

# 3. Налаштування Java (критично для SparkSubmit)
docker exec -it --user root task2_batch-airflow-scheduler-1 bash -c "apt-get update && apt-get install -y default-jdk"
