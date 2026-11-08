"""Фінальний проєкт. Частина 2, крок 4: Airflow DAG,
що послідовно запускає landing_to_bronze -> bronze_to_silver -> silver_to_gold.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Spark-скрипти лежать поруч із цим файлом (папка dags)
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
# Локальне з'єднання Spark (local[*]) задане змінною AIRFLOW_CONN_SPARK_DEFAULT
SPARK_CONN_ID = "spark_default"

default_args = {
    "owner": "polina_krashchenko",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="polina_krashchenko_fp_datalake",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["polina_krashchenko"],
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=f"{SCRIPTS_DIR}/landing_to_bronze.py",
        conn_id=SPARK_CONN_ID,
        verbose=1,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=f"{SCRIPTS_DIR}/bronze_to_silver.py",
        conn_id=SPARK_CONN_ID,
        verbose=1,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=f"{SCRIPTS_DIR}/silver_to_gold.py",
        conn_id=SPARK_CONN_ID,
        verbose=1,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
