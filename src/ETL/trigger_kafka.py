from databases.mysql_connect import MySQLConnect
from config.database_config import get_database_config, get_kafka_config
from kafka import KafkaProducer

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
        cursor.execute("sELECT * FROM users_log_before")
        print(cursor.fetchall())
        connection.commit()
    
    producer = KafkaProducer(
        bootstrap_servers=configKafka["bootstrap_servers"],
        value_serializer=lambda v: str(v).encode("utf-8")
    )