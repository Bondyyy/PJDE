from dotenv import load_dotenv
import os
from dataclasses import dataclass

@dataclass
class MySQLConfig():
    host: str
    port: int
    user: str
    password: str

@dataclass
class MongoConfig():
    uri: str
    db_name: str

def get_database_config():
    load_dotenv()
    config = {
        "mysql": MySQLConfig(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT")),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD")
        ),
        "mongo": MongoConfig(
            uri=os.getenv("MONGO_URI"),
            db_name=os.getenv("MONGO_DB_NAME")
        )
    }
    return config

if __name__ == "__main__":
    config = get_database_config()
    print(config)