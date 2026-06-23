from databases.mysql_connect import MySQLConnect
from config.database_config import get_mysql_config
from databases.schema_manager import create_mysql_schema, validate_mysql_schema

def main(config):
    with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user, config["mysql"].password) as mysql_conn:
        connection, cursor = mysql_conn.connection, mysql_conn.cursor
        create_mysql_schema(connection, cursor)
        cursor.execute(
            """
            INSERT INTO users (user_id, login, gravatar_id, url, avatar_url)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                login = VALUES(login),
                gravatar_id = VALUES(gravatar_id),
                url = VALUES(url),
                avatar_url = VALUES(avatar_url)
            """,
            (
                1,
                "octocat",
                "gravatar_id",
                "https://github.com/users",
                "https://avatars.github.com/u/"
            )
        )
        connection.commit()
        validate_mysql_schema(cursor)
    
if __name__ == "__main__":
    config = get_mysql_config()
    main(config)
    