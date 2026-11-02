# goit-de-fp — фінальний проєкт

## Частина 1. End-to-End Streaming Pipeline (`part1/`)

| Файл | Призначення |
|---|---|
| `configs.py` | підключення до Kafka і MySQL, імена топіків і вихідної таблиці |
| `create_topics.py` | створює Kafka-топіки |
| `streaming_pipeline.py` | основний конвеєр, етапи 1–6 позначені коментарями |
| `kafka_reader.py` | виводить дані з вихідного Kafka-топіку |

Етапи в `streaming_pipeline.py`:
1. Читання `olympic_dataset.athlete_bio` з MySQL через Spark JDBC.
2. Фільтрація: зріст і вага мають бути заповнені й бути числами (`try_cast` + `isNotNull`).
3. `athlete_event_results` з MySQL записується в Kafka-топік, потім читається як стрим; JSON розгортається в окремі колонки.
4. Join результатів змагань з біоданими за `athlete_id`.
5. Середні `avg_height` і `avg_weight` для кожної комбінації `sport`, `medal` (або `No medal`), `sex`, `country_noc` + `timestamp`.
6. `forEachBatch`: а) запис у вихідний Kafka-топік, b) запис у MySQL-таблицю.

Запуск:
```bash
pip install -r part1/requirements.txt
cd part1
python create_topics.py
python streaming_pipeline.py
python kafka_reader.py      # окремий термінал
```

## Частина 2. End-to-End Batch Data Lake (`part2/`)

| Файл | Що робить |
|---|---|
| `landing_to_bronze.py` | завантажує `athlete_bio.csv` і `athlete_event_results.csv` з FTP, зберігає parquet у `bronze/{table}` |
| `bronze_to_silver.py` | чистить усі текстові колонки, видаляє дублікати, зберігає у `silver/{table}` |
| `silver_to_gold.py` | join за `athlete_id`, середні `weight` і `height` за `sport`, `medal`, `sex`, `country_noc` + `timestamp`, зберігає у `gold/avg_stats` |
| `project_solution.py` | Airflow DAG `polina_krashchenko_fp_datalake`: `landing_to_bronze → bronze_to_silver → silver_to_gold` (`SparkSubmitOperator`) |

Кожна Spark-задача виводить фінальний DataFrame через `df.show()`.

Локальний запуск без Airflow:
```bash
pip install -r part2/requirements.txt
cd part2
python landing_to_bronze.py && python bronze_to_silver.py && python silver_to_gold.py
```
