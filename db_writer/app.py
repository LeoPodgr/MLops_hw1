import json
import logging
import os
import time

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("db_writer")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")
PG = dict(
    host=os.getenv("PG_HOST", "postgres"),
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB", "fraud"),
    user=os.getenv("PG_USER", "fraud"),
    password=os.getenv("PG_PASSWORD", "fraud"),
)

INSERT_SQL = """
INSERT INTO scores (transaction_id, score, fraud_flag)
VALUES (%s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING
"""


def connect_pg():
    while True:
        try:
            conn = psycopg2.connect(**PG)
            conn.autocommit = True
            log.info("Connected to Postgres")
            return conn
        except Exception as e:
            log.warning("Postgres not ready: %s", e)
            time.sleep(2)


def connect_kafka():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_SERVERS,
                group_id="db-writer",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            log.info("Subscribed to topic %s", TOPIC)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready")
            time.sleep(2)


def main():
    conn = connect_pg()
    consumer = connect_kafka()
    for msg in consumer:
        v = msg.value
        try:
            with conn.cursor() as cur:
                cur.execute(
                    INSERT_SQL,
                    (v["transaction_id"], float(v["score"]), int(v["fraud_flag"])),
                )
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            log.exception("Lost Postgres connection, reconnecting")
            conn = connect_pg()
        except Exception:
            log.exception("Bad message skipped: %s", v)


if __name__ == "__main__":
    main()