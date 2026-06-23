def create_mysql_schema(connection, cursor):
    database_name = "github_data"
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    print(f"------Created database {database_name}------")