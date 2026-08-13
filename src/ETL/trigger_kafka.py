from databases.mysql_connect import MySQLConnect
from config.database_config import get_database_config, get_kafka_config
from kafka import KafkaProducer
import json
import time

RAW_TOPIC = "BondyPJDE_raw"
LOG_SOURCES = {
    "users": {
        "table": "users_log",
        "columns": ["log_id", "user_id", "login", "gravatar_id", "url", "avatar_url", "state", "log_timestamp"]
    },
    "repositories": {
        "table": "repositories_log",
        "columns": ["log_id", "repo_id", "name", "url", "state", "log_timestamp"],
    }
}

def fetch_logs(cursor, table_name, columns, last_log_id):
    selected_columns = ", ".join(f"`{column}`" for column in columns)
    cursor.execute(
        f"""
            SELECT {selected_columns}
            FROM `{table_name}`
            WHERE log_id > %s
            ORDER BY log_id
        """,
        (last_log_id,),
    )
    return cursor.fetchall()

def load_checkpoints(cursor):
    cursor.execute("""SELECT entity, last_log_id
        FROM kafka_checkpoint
    """)

    return {
        entity: last_log_id
        for entity, last_log_id in cursor.fetchall()
    }
    
def save_checkpoint(cursor,entity,last_log_id):
    cursor.execute("""
        UPDATE kafka_checkpoint
        SET last_log_id = %s
        WHERE entity = %s
    """, (
        last_log_id,
        entity
    ))
def trigger_kafka():
    configDB = get_database_config()
    configKafka = get_kafka_config()

    producer = KafkaProducer(
        bootstrap_servers=configKafka["bootstrap_servers"],
        value_serializer=lambda x: json.dumps(
            x,
            default=str
        ).encode("utf-8")
    )

    try:
        with MySQLConnect(configDB["mysql"].host, configDB["mysql"].port, configDB["mysql"].user, configDB["mysql"].password) as mysql_conn:

            connection = mysql_conn.connection
            cursor = mysql_conn.cursor
            database = configDB["mysql"].database

            cursor.execute(f"USE `{database}`")
            connection.autocommit = True

            last_log_ids = load_checkpoints(cursor)

            while True:
                for entity, source in LOG_SOURCES.items():
                    rows = fetch_logs(cursor, source["table"], source["columns"], last_log_ids[entity])

                    if not rows:
                        continue

                    for row in rows:
                        message = dict(zip(source["columns"], row))
                        message["entity"] = entity
                        producer.send(RAW_TOPIC, message)

                    producer.flush()
                    last_log_ids[entity] = rows[-1][0]
                    save_checkpoint(cursor, entity, last_log_ids[entity])
                    print(f"Sent {len(rows)} {entity} records")
                    print(
                        f"Last {entity} log id: {last_log_ids[entity]}"
                    )
                time.sleep(1)
    finally:
        producer.close()


def main():
    trigger_kafka()


if __name__ == "__main__":
    main()
