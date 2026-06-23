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
    validate_expected_tables(cursor)
    validate_expected_columns(cursor)
    validate_minimum_row_counts(cursor)
    validate_required_fields(cursor)
    validate_unique_fields(cursor)
    validate_url_format(cursor)

    print("------All database validations passed successfully------")


def validate_expected_tables(cursor):
    expected_tables = {
        "users",
        "repositories",
    }

    cursor.execute("SHOW TABLES")
    existing_tables = {row[0] for row in cursor.fetchall()}

    missing_tables = expected_tables - existing_tables

    if missing_tables:
        raise Exception(
            f"--------Validation failed. Missing tables: {missing_tables}--------"
        )


def validate_expected_columns(cursor):
    expected_columns = {
        "users": {
            "user_id",
            "login",
            "gravatar_id",
            "url",
            "avatar_url",
        },
        "repositories": {
            "repo_id",
            "name",
            "url",
        },
    }

    for table_name, required_columns in expected_columns.items():
        cursor.execute(f"DESCRIBE `{table_name}`")
        existing_columns = {row[0] for row in cursor.fetchall()}

        missing_columns = required_columns - existing_columns

        if missing_columns:
            raise Exception(
                f"--------Validation failed. Table '{table_name}' missing columns: {missing_columns}--------"
            )


def validate_minimum_row_counts(cursor):
    expected_min_rows = {
        "users": 1,
        "repositories": 0,
    }

    for table_name, min_rows in expected_min_rows.items():
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        row_count = cursor.fetchone()[0]

        if row_count < min_rows:
            raise Exception(
                f"--------Validation failed. Table '{table_name}' expected at least "
                f"{min_rows} rows, found {row_count}--------"
            )


def validate_required_fields(cursor):
    required_fields = {
        "users": ["user_id", "login"],
        "repositories": ["repo_id", "name"],
    }

    for table_name, columns in required_fields.items():
        conditions = []

        for column in columns:
            conditions.append(f"`{column}` IS NULL")

            if column not in {"user_id", "repo_id"}:
                conditions.append(f"TRIM(`{column}`) = ''")

        where_clause = " OR ".join(conditions)

        cursor.execute(f"""
            SELECT COUNT(*)
            FROM `{table_name}`
            WHERE {where_clause}
        """)

        invalid_count = cursor.fetchone()[0]

        if invalid_count > 0:
            raise Exception(
                f"--------Validation failed. Table '{table_name}' has "
                f"{invalid_count} rows with NULL or empty required fields--------"
            )


def validate_unique_fields(cursor):
    unique_fields = {
        "users": ["user_id", "login"],
        "repositories": ["repo_id"],
    }

    for table_name, columns in unique_fields.items():
        for column in columns:
            cursor.execute(f"""
                SELECT `{column}`, COUNT(*) AS duplicate_count
                FROM `{table_name}`
                GROUP BY `{column}`
                HAVING COUNT(*) > 1
                LIMIT 1
            """)

            duplicate = cursor.fetchone()

            if duplicate:
                raise Exception(
                    f"--------Validation failed. Duplicate value found in "
                    f"'{table_name}.{column}': {duplicate}--------"
                )


def validate_url_format(cursor):
    url_fields = {
        "users": ["url", "avatar_url"],
        "repositories": ["url"],
    }

    for table_name, columns in url_fields.items():
        for column in columns:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM `{table_name}`
                WHERE `{column}` IS NOT NULL
                  AND `{column}` != ''
                  AND `{column}` NOT LIKE 'http%'
            """)

            invalid_url_count = cursor.fetchone()[0]

            if invalid_url_count > 0:
                raise Exception(
                    f"--------Validation failed. Table '{table_name}', column '{column}' "
                    f"has {invalid_url_count} invalid URLs--------"
                )