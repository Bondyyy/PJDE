from pathlib import Path

SQL_FILE_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"

def create_mysql_schema(connection, cursor):
    
    database_name = "github_data"
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    connection.commit()
    print(f"------Created database {database_name} successfully------")
    connection.database = database_name
    
    try:
        with open(SQL_FILE_PATH, "r") as sql_file:
            sql_script = sql_file.read()
            sql_commands = [command.strip() for command in sql_script.split(";") if command.strip()]
            for command in sql_commands:
                cursor.execute(command)
                connection.commit()
                print(f"------Executed SQL command successfully------")
    except Exception as e:
        connection.rollback()
        raise Exception(f"--------Error executing SQL script: {e} --------") from e
    
def validate_mysql_schema(cursor):
    # Validate expected tables exist
    expected_tables = {
        "users",
        "repositories"
    }
    cursor.execute("SHOW TABLES")
    existing_tables = {row[0] for row in cursor.fetchall()}
    missing_tables = expected_tables - existing_tables

    if missing_tables:
        raise Exception(
            f"--------Validation failed. Missing tables: {missing_tables}--------"
        )
    print(f"------Validated database successfully. Tables: {existing_tables}------")
    
    # Validate that the 'users' table has data
    cursor.execute("SELECT COUNT(*) FROM users")
    user_count = cursor.fetchone()[0]

    if user_count == 0:
        raise Exception("--------Validation failed. No data found in 'users' table--------")

    cursor.execute("SELECT * FROM users LIMIT 1")
    sample_user = cursor.fetchone()

    print(
        f"------Validated data in 'users' table successfully. "
        f"Total users: {user_count}. Sample data: {sample_user}------"
    )