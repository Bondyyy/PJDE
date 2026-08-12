from databases.mysql_connect import MySQLConnect
from config.database_config import get_database_config, get_kafka_config
from kafka import KafkaProducer
import json
import time

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

    with MySQLConnect(
        configDB["mysql"].host,
        configDB["mysql"].port,
        configDB["mysql"].user,
        configDB["mysql"].password
    ) as mysql_conn:

        connection = mysql_conn.connection
        cursor = mysql_conn.cursor
        database = configDB["mysql"].database

        cursor.execute(f"USE {database}")

        connection.autocommit = True

        last_log_id = 0
        columns = ["log_id", "user_id", "login", "gravatar_id", "url", "avatar_url",
            "state", "log_timestamp"
        ]
        while True:
            cursor.execute("""
                SELECT log_id, user_id, login, gravatar_id, url,
                    avatar_url, state, log_timestamp
                FROM users_log_before
                WHERE log_id > %s
                ORDER BY log_id
            """, (last_log_id,))
            data = cursor.fetchall()
            
            if data:
                for row in data:
                    message = dict(zip(columns, row))
                    producer.send("BondyPJDE", message)

                producer.flush()

                last_log_id = data[-1][0]

                print(f"Sent {len(data)} records")
                print(f"Last log id: {last_log_id}")


def main():
    trigger_kafka()


if __name__ == "__main__":
    main()