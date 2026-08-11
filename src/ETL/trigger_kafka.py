from databases.mysql_connect import MySQLConnect
from config.database_config import get_database_config, get_kafka_config
from kafka import KafkaProducer
import json

configDB = get_database_config()
configKafka = get_kafka_config()

with MySQLConnect(
        configDB["mysql"].host,
        configDB["mysql"].port,
        configDB["mysql"].user,
        configDB["mysql"].password
    ) as mysql_conn:
    
        connection, cursor = mysql_conn.connection, mysql_conn.cursor
        database = configDB["mysql"].database
        connection.database = database

        cursor.execute(f"USE {database}")
        cursor.execute("""SELECT user_id, login, gravatar_id, url, avatar_url, state, 
                                    date_format(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') as log_timestamp
                        FROM users_log_before""")
        print(cursor.fetchall())
        connection.commit()
    
        data = cursor.fetchall()
        producer = KafkaProducer(
            bootstrap_servers=configKafka["bootstrap_servers"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
        producer.send("BondyPJDE", data)
        producer.flush()
        print(data)
        
        