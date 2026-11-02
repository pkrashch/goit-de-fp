"""Фінальний проєкт. Частина 2, крок 1: landing -> bronze.

Завантажує CSV з FTP-сервера, читає його Spark-ом і зберігає у parquet: bronze/{table}.
"""
import os

import requests
from pyspark.sql import SparkSession

FTP_URL = "https://ftp.goit.study/neoversity/"
TABLES = ["athlete_bio", "athlete_event_results"]
LANDING_DIR = "landing"


def download_data(table):
    """Завантажує {table}.csv з FTP у папку landing (в оригінальному форматі)."""
    os.makedirs(LANDING_DIR, exist_ok=True)
    local_path = os.path.join(LANDING_DIR, f"{table}.csv")
    url = f"{FTP_URL}{table}.csv"
    print(f"Downloading from {url}")
    response = requests.get(url, timeout=120)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to download {url}. Status code: {response.status_code}")
    with open(local_path, "wb") as file:
        file.write(response.content)
    print(f"File downloaded successfully and saved as {local_path}")
    return local_path


def main():
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    for table in TABLES:
        local_path = download_data(table)

        df = spark.read.csv(local_path, header=True, inferSchema=True)

        output_path = os.path.join("bronze", table)
        df.write.mode("overwrite").parquet(output_path)
        print(f"Saved {table} to {output_path}")

        df = spark.read.parquet(output_path)
        df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
