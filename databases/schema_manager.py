from pathlib import Path

SQL_FILE_PATH = Path(__file__).parent / "schema.sql"
def create_mysql_schema(connection, cursor):
    database_name = "github_data"
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    print(f"------Created database {database_name} successfully------")
    connection.database = database_name
    with open(SQL_FILE_PATH, "r") as sql_file:
        sql_script = sql_file.read()
        sql_commands = [command.strip() for command in sql_script.split(";") if command.strip()]
        for command in sql_commands:
            cursor.execute(command)
            print(f"------Executed SQL command: {command}------")