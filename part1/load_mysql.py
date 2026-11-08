"""Підготовка частини 1: завантажує CSV курсу з FTP і записує їх у локальний MySQL
(olympic_dataset.athlete_bio і olympic_dataset.athlete_event_results).
Запускається один раз перед streaming_pipeline.py.
"""
import os

import requests
from pyspark.sql import SparkSession

from configs import FTP_URL, MYSQL_CONNECTOR, SOURCE_TABLES, jdbc_url, mysql_config

DATA_DIR = "data"


def download(table):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, f"{table}.csv")
    if not os.path.exists(path):
        url = f"{FTP_URL}{table}.csv"
        print(f"Downloading {url}")
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        with open(path, "wb") as f:
            f.write(response.content)
    return path


spark = (SparkSession.builder
         .appName("LoadMySQL")
         .master("local[*]")
         .config("spark.jars.packages", MYSQL_CONNECTOR)
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

for table in SOURCE_TABLES:
    df = spark.read.csv(download(table), header=True, multiLine=True, escape='"')
    (df.write
     .format("jdbc")
     .option("url", jdbc_url())
     .option("driver", "com.mysql.cj.jdbc.Driver")
     .option("dbtable", table)
     .option("user", mysql_config["user"])
     .option("password", mysql_config["password"])
     .mode("overwrite")
     .save())
    print(f"olympic_dataset.{table}: {df.count()} rows loaded")

spark.stop()
