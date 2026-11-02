"""Налаштування фінального проєкту, частина 1."""

# Kafka (сервер курсу)
kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# MySQL (сервер курсу). Логін і пароль надає ментор.
mysql_config = {
    "host": "217.61.57.46",
    "port": 3306,
    "database": "olympic_dataset",
    "user": "MYSQL_USER",
    "password": "MYSQL_PASSWORD",
}

MY_NAME = "polina_krashchenko"

# Kafka-топіки
TOPIC_EVENT_RESULTS = f"{MY_NAME}_athlete_event_results"
TOPIC_ENRICHED = f"{MY_NAME}_enriched_athlete_avg"

# Вихідна таблиця в MySQL
OUTPUT_TABLE = f"{MY_NAME}_enriched_athlete_avg"


def jdbc_url():
    return (f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/"
            f"{mysql_config['database']}")


def kafka_spark_options():
    """Параметри Kafka для Spark (readStream / write)."""
    jaas = ("org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";')
    return {
        "kafka.bootstrap.servers": ",".join(kafka_config["bootstrap_servers"]),
        "kafka.security.protocol": kafka_config["security_protocol"],
        "kafka.sasl.mechanism": kafka_config["sasl_mechanism"],
        "kafka.sasl.jaas.config": jaas,
    }


def kafka_python_params():
    """Параметри Kafka для kafka-python (адмін-клієнт / консюмер)."""
    return {
        "bootstrap_servers": kafka_config["bootstrap_servers"],
        "security_protocol": kafka_config["security_protocol"],
        "sasl_mechanism": kafka_config["sasl_mechanism"],
        "sasl_plain_username": kafka_config["username"],
        "sasl_plain_password": kafka_config["password"],
    }
