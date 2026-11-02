"""Фінальний проєкт. Частина 2, крок 2: bronze -> silver.

Читає bronze/{table}, чистить усі текстові колонки, видаляє дублікати
і зберігає результат у silver/{table}.
"""
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

TABLES = ["athlete_bio", "athlete_event_results"]


def clean_text(text):
    """Функція чистки тексту з матеріалів курсу (додатково залишаємо пробіли між словами)."""
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\\"\' ]', '', str(text)).strip()


clean_text_udf = udf(clean_text, StringType())


def main():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    for table in TABLES:
        df = spark.read.parquet(os.path.join("bronze", table))

        # Чистка тексту для всіх текстових колонок
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, clean_text_udf(df[field.name]))

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
