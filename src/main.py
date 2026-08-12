from databases.mysql_connect import MySQLConnect
from databases.mongodb_connect import MongoDBConnection
from config.database_config import get_database_config
from databases.schema_manager import create_mysql_schema,create_mysql_triggers,validate_mysql_schema,validate_mysql_triggers,create_mongo_schema,validate_mongo_schema

def main(config):

    with MySQLConnect(config["mysql"].host,config["mysql"].port,config["mysql"].user, config["mysql"].password) as mysql_conn:
        connection = mysql_conn.connection
        cursor = mysql_conn.cursor
        # 1. Tạo database + main tables
        create_mysql_schema(connection, cursor)
        # 2. Tạo log tables + triggers
        create_mysql_triggers(connection, cursor)
        # 3. Validate MySQL
        validate_mysql_schema(cursor)
        validate_mysql_triggers(cursor)

    with MongoDBConnection(config["mongo"].uri,config["mongo"].db_name) as mongo_client:
        # 4. Tạo collections
        create_mongo_schema(
            mongo_client.db
        )
        # 5. Validate MongoDB
        validate_mongo_schema(
            mongo_client.db
        )
    print("------Database setup completed successfully------")

if __name__ == "__main__":
    config = get_database_config()
    main(config)