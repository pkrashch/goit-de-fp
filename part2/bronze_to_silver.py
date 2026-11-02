"""Фінальний проєкт. Частина 2, крок 2: bronze -> silver.

Читає bronze/{table}, чистить усі текстові колонки, видаляє дублікати
і зберігає результат у silver/{table}.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

TABLES = ["athlete_bio", "athlete_event_results"]


def clean_text(column):
    """Залишає лише літери, цифри, пробіли та базову пунктуацію."""
    return F.trim(F.regexp_replace(column, r"[^a-zA-Z0-9,.\\\"' ]", ""))


def main():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    for table in TABLES:
        df = spark.read.parquet(os.path.join("bronze", table))

        # Чистка тексту для всіх текстових колонок
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, clean_text(F.col(field.name)))

        # Дедублікація рядків
        df = df.dropDuplicates()

        output_path = os.path.join("silver", table)
        df.write.mode("overwrite").parquet(output_path)
        print(f"Saved {table} to {output_path}")

        df = spark.read.parquet(output_path)
        df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
