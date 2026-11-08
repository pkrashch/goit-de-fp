# goit-de-fp — фінальний проєкт

За погодженням з ментором проєкт запускається локально в Docker (доступу до серверів курсу немає):
MySQL і Airflow зі Spark піднімаються з `docker-compose.yml`, Kafka — сервер курсу.

## Частина 1. End-to-End Streaming Pipeline (`part1/`)

| Файл | Призначення |
|---|---|
| `configs.py` | підключення до Kafka (сервер курсу) і MySQL (локальний контейнер), імена топіків і таблиці |
| `load_mysql.py` | підготовка: завантажує `athlete_bio.csv` і `athlete_event_results.csv` з FTP курсу в MySQL |
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

## Частина 2. End-to-End Batch Data Lake (`part2/`)

| Файл | Що робить |
|---|---|
| `landing_to_bronze.py` | завантажує CSV з FTP, зберігає parquet у `bronze/{table}` |
| `bronze_to_silver.py` | чистить усі текстові колонки (UDF `clean_text`), видаляє дублікати, зберігає у `silver/{table}` |
| `silver_to_gold.py` | join за `athlete_id`, середні `weight` і `height` за `sport`, `medal`, `sex`, `country_noc` + `timestamp`, зберігає у `gold/avg_stats` |
| `project_solution.py` | Airflow DAG `polina_krashchenko_fp_datalake`: `landing_to_bronze → bronze_to_silver → silver_to_gold` (`SparkSubmitOperator`) |

Кожна Spark-задача виводить фінальний DataFrame через `df.show()` — результат видно в логах задач Airflow.

## Локальне середовище (`docker-compose.yml`)

- `mysql` — MySQL 8.0, порт `3307` на хості, база `olympic_dataset`, користувач `fp_user`.
- `airflow` — `apache/airflow:2.8.1` + Java + PySpark 3.5 (`docker/Dockerfile.airflow`), команда `standalone`;
  папка `part2/` змонтована як `dags/`, з'єднання `spark_default` (`local[*]`) задане змінною
  `AIRFLOW_CONN_SPARK_DEFAULT`; дані data lake пишуться в `datalake/`.

## Запуск

```bash
docker compose up -d --build
```

**Частина 1** (на хості, потрібні Java 17 і Python):
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r part1/requirements.txt
cd part1
python load_mysql.py          # один раз: CSV -> MySQL
python create_topics.py
python streaming_pipeline.py  # термінал 1
python kafka_reader.py        # термінал 2
```

**Частина 2**: http://localhost:8080 (логін `admin`, пароль —
`docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt`), запустити DAG
`polina_krashchenko_fp_datalake`.
