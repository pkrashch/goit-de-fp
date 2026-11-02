"""Фінальний проєкт. Частина 1: End-to-End Streaming Pipeline.

MySQL (athlete_bio, athlete_event_results) -> Kafka -> Spark Structured Streaming
-> середні зріст і вага -> forEachBatch -> Kafka-топік + MySQL-таблиця.
"""
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from configs import (OUTPUT_TABLE, TOPIC_ENRICHED, TOPIC_EVENT_RESULTS, jdbc_url,
                     kafka_spark_options, mysql_config)

PACKAGES = ",".join([
    f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}",
    "com.mysql:mysql-connector-j:8.4.0",
])

spark = (SparkSession.builder
         .appName("FinalProjectStreaming")
         .master("local[*]")
         .config("spark.jars.packages", PACKAGES)
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

jdbc_options = {
    "url": jdbc_url(),
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": mysql_config["user"],
    "password": mysql_config["password"],
}
kafka_options = kafka_spark_options()


def read_mysql_table(table):
    return spark.read.format("jdbc").options(**jdbc_options).option("dbtable", table).load()


# ---------------------------------------------------------------
# Етап 1: зчитування фізичних показників атлетів з MySQL (athlete_bio)
# ---------------------------------------------------------------
bio_df = read_mysql_table("athlete_bio")

# ---------------------------------------------------------------
# Етап 2: фільтрація — зріст і вага мають бути заповнені й бути числами
# ---------------------------------------------------------------
bio_df = (bio_df
          .withColumn("height", F.expr("try_cast(height AS DOUBLE)"))
          .withColumn("weight", F.expr("try_cast(weight AS DOUBLE)"))
          .where(F.col("height").isNotNull() & F.col("weight").isNotNull())
          .select("athlete_id", "sex", "height", "weight"))

# ---------------------------------------------------------------
# Етап 3.1: athlete_event_results з MySQL -> Kafka-топік
# ---------------------------------------------------------------
events_mysql_df = read_mysql_table("athlete_event_results")

(events_mysql_df
 .select(F.col("athlete_id").cast("string").alias("key"),
         F.to_json(F.struct(*events_mysql_df.columns)).alias("value"))
 .write
 .format("kafka")
 .options(**kafka_options)
 .option("topic", TOPIC_EVENT_RESULTS)
 .save())
print(f"athlete_event_results written to Kafka topic '{TOPIC_EVENT_RESULTS}'")

# ---------------------------------------------------------------
# Етап 3.2: читання результатів змагань з Kafka-топіку,
#           JSON -> DataFrame (кожне поле JSON — окрема колонка)
# ---------------------------------------------------------------
events_schema = StructType([StructField(c, StringType()) for c in events_mysql_df.columns])

events_stream = (spark.readStream
                 .format("kafka")
                 .options(**kafka_options)
                 .option("subscribe", TOPIC_EVENT_RESULTS)
                 .option("startingOffsets", "earliest")
                 .option("maxOffsetsPerTrigger", "20000")
                 .load()
                 .select(F.from_json(F.col("value").cast("string"), events_schema).alias("data"))
                 .select("data.*")
                 .withColumn("athlete_id", F.col("athlete_id").cast("int")))

# ---------------------------------------------------------------
# Етап 4: об'єднання результатів змагань з біоданими за athlete_id
# ---------------------------------------------------------------
joined_stream = events_stream.join(bio_df, on="athlete_id", how="inner")

# ---------------------------------------------------------------
# Етап 5: середній зріст і вага для кожного виду спорту, типу медалі
#         (або її відсутності), статі та країни + timestamp розрахунку
# ---------------------------------------------------------------
avg_stream = (joined_stream
              .withColumn("medal", F.coalesce(F.col("medal"), F.lit("No medal")))
              .groupBy("sport", "medal", "sex", "country_noc")
              .agg(F.avg("height").alias("avg_height"),
                   F.avg("weight").alias("avg_weight"))
              .withColumn("timestamp", F.current_timestamp()))


# ---------------------------------------------------------------
# Етап 6: стрим даних за допомогою forEachBatch
# ---------------------------------------------------------------
def foreach_batch_function(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    print(f"Batch {batch_id}: {batch_df.count()} rows")
    batch_df.show(10, truncate=False)

    # Етап 6.а): запис у вихідний Kafka-топік
    (batch_df
     .select(F.to_json(F.struct(*batch_df.columns)).alias("value"))
     .write
     .format("kafka")
     .options(**kafka_options)
     .option("topic", TOPIC_ENRICHED)
     .save())

    # Етап 6.b): запис у базу даних
    (batch_df.write
     .format("jdbc")
     .options(**jdbc_options)
     .option("dbtable", OUTPUT_TABLE)
     .mode("append")
     .save())


query = (avg_stream.writeStream
         .outputMode("complete")
         .foreachBatch(foreach_batch_function)
         .option("checkpointLocation", "checkpoints/enriched_athlete_avg")
         .trigger(processingTime="30 seconds")
         .start())

print(f"Streaming '{TOPIC_EVENT_RESULTS}' -> '{TOPIC_ENRICHED}' + MySQL '{OUTPUT_TABLE}'. Ctrl+C to stop.")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()
