from dotenv import load_dotenv
import os
from dataclasses import dataclass

@dataclass
class MySQLConfig():
    host: str
    port: int
    user: str
    password: str
    database: str
    table: str = "users"

@dataclass
class MongoConfig():
    uri: str
    db_name: str

def get_kafka_config():
    load_dotenv()
    config = {
        "bootstrap_servers": os.getenv("BootstrapServers")
    }
    return config

def get_database_config():
    load_dotenv()
    config = {
        "mysql": MySQLConfig(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT")),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB_NAME")
        ),
        "mongo": MongoConfig(
            uri=os.getenv("MONGO_URI"),
            db_name=os.getenv("MONGO_DB_NAME")
        )
    }
    return config

def get_spark_config():
    db_config = get_database_config()
    
    return {
        "mysql": {
            "table": db_config["mysql"].table,
            "jdbc_url": f"jdbc:mysql://{db_config['mysql'].host}:{db_config['mysql'].port}/{db_config['mysql'].database}",
            "config": {
                "host": db_config["mysql"].host,
                "port": db_config["mysql"].port,
                "user": db_config["mysql"].user,
                "password": db_config["mysql"].password,
                "database": db_config["mysql"].database
            }   
        },
        "mongo": {
            "collection": "users",
            "uri": db_config["mongo"].uri,
            "database": db_config["mongo"].db_name
        },
        "redis": {}
    }
    

if __name__ == "__main__":
    config = get_database_config()
    print(config)