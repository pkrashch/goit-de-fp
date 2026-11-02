"""Фінальний проєкт. Частина 2, крок 3: silver -> gold.

Об'єднує silver/athlete_bio і silver/athlete_event_results за athlete_id,
рахує середні weight і height для кожної комбінації sport, medal, sex, country_noc,
додає timestamp і зберігає результат у gold/avg_stats.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    bio_df = (spark.read.parquet(os.path.join("silver", "athlete_bio"))
              .withColumn("height", F.expr("try_cast(height AS DOUBLE)"))
              .withColumn("weight", F.expr("try_cast(weight AS DOUBLE)"))
              # country_noc беремо з результатів змагань, тому з біоданих його прибираємо
              .drop("country_noc"))
    events_df = spark.read.parquet(os.path.join("silver", "athlete_event_results"))

    joined_df = events_df.join(bio_df, on="athlete_id", how="inner")

    avg_stats = (joined_df
                 .groupBy("sport", "medal", "sex", "country_noc")
                 .agg(F.avg("weight").alias("avg_weight"),
                      F.avg("height").alias("avg_height"))
                 .withColumn("timestamp", F.current_timestamp()))

    output_path = os.path.join("gold", "avg_stats")
    avg_stats.write.mode("overwrite").parquet(output_path)
    print(f"Saved avg_stats to {output_path}")

    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
