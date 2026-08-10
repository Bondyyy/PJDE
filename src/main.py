from databases.mysql_connect import MySQLConnect
from config.database_config import get_database_config
from databases.schema_manager import create_mongo_schema, create_mysql_schema, validate_mysql_schema, validate_mongo_schema
from databases.mongodb_connect import MongoDBConnection
from bson.int64 import Int64

def main(config):
    with MySQLConnect(
        config["mysql"].host,
        config["mysql"].port,
        config["mysql"].user,
        config["mysql"].password
    ) as mysql_conn:

        connection, cursor = mysql_conn.connection, mysql_conn.cursor

        # MySQL
        create_mysql_schema(connection, cursor)
        connection.commit()
        validate_mysql_schema(cursor)

        # MongoDB
        with MongoDBConnection(config["mongo"].uri, config["mongo"].db_name) as mongo_client:
            create_mongo_schema(mongo_client.db)
            # for user_id in range(1, 6):
            #     user_id =  Int64(user_id)  
            #     mongo_client.db.users.update_one(
            #         {"user_id": user_id},
            #         {
            #             "$set": {
            #                 "user_id": user_id,
            #                 "login": f"octocat_{user_id}",
            #                 "gravatar_id": f"gravatar_id_{user_id}",
            #                 "url": f"https://github.com/users/{user_id}",
            #                 "avatar_url": f"https://avatars.github.com/u/{user_id}"
            #             }
            #         },
            #         upsert=True
            #     )
            # print("------Inserted into MongoDB collection successfully------")
            validate_mongo_schema(mongo_client.db)


if __name__ == "__main__":
    config = get_database_config()
    main(config)