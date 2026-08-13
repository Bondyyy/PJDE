from pathlib import Path

SQL_FILE_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
TRIGGER_FILE_PATH =  Path(__file__).resolve().parent.parent/ "sql"/ "trigger.sql"

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

def create_mysql_triggers(connection, cursor):
    try:
        with open(TRIGGER_FILE_PATH, "r", encoding="utf-8") as sql_file:
            sql_script = sql_file.read()
        delimiter = ";"
        statement_lines = []

        for raw_line in sql_script.splitlines():
            line = raw_line.strip()
            # Bỏ qua dòng rỗng
            if not line:
                continue
            # Đổi delimiter khi gặp: DELIMITER //
            if line.upper().startswith("DELIMITER "):
                delimiter = line.split(None, 1)[1]
                continue

            statement_lines.append(raw_line)
            statement = "\n".join(statement_lines).strip()
            # Chỉ execute khi gặp delimiter hiện tại
            if statement.endswith(delimiter):
                statement = statement[:-len(delimiter)].strip()
                if statement:
                    cursor.execute(statement)
                statement_lines = []

        # Phòng trường hợp file còn câu SQL chưa execute
        if statement_lines:
            statement = "\n".join(statement_lines).strip()
            if statement:
                cursor.execute(statement)
        connection.commit()
        print("------Created MySQL logs and triggers successfully------")
    except Exception as e:
        connection.rollback()
        raise Exception(
            f"--------Error creating MySQL triggers: {e}--------"
        ) from e

def validate_mysql_triggers(cursor):
    expected_triggers = {"after_insert_user_log", "after_update_user_log", "after_delete_user_log", 
                         "after_insert_repository_log", "after_update_repository_log", "after_delete_repository_log",}
    cursor.execute("SHOW TRIGGERS")

    existing_triggers = {
        row[0]
        for row in cursor.fetchall()
    }

    missing_triggers = (
        expected_triggers - existing_triggers
    )

    if missing_triggers:
        raise Exception(f"--------Missing MySQL triggers: {missing_triggers}--------")
    print(f"------MySQL triggers: {existing_triggers}------")
    
def validate_mysql_schema(cursor):
    expected_tables = {"users","repositories"}
    
    cursor.execute("SHOW TABLES")
    existing_tables = {row[0] for row in cursor.fetchall()}

    missing_tables = expected_tables - existing_tables

    if missing_tables:
        raise Exception(f"--------Validation failed. Missing tables: {missing_tables}--------")
    print(f"------MySQL collections: {existing_tables}------")


def create_collection(db, collection_name, validator, index_field, unique_index):
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name, validator=validator)
        action = "Created"
    else:
        db.command(
            "collMod",
            collection_name,
            validator=validator,
        )
        action = "Updated"

    db[collection_name].create_index(index_field, unique=unique_index)
    print(
        f"------{action} MongoDB collection '{collection_name}' successfully------"
    )


def create_mongo_schema(db):
    users_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["user_id", "login"],
            "properties": {
                "user_id": {"bsonType": "long"},
                "login": {"bsonType": "string"},
                "gravatar_id": {"bsonType": "string"},
                "url": {"bsonType": "string"},
                "avatar_url": {"bsonType": "string"},
            },
        }
    }
    repositories_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["repo_id", "name"],
            "properties": {
                "repo_id": {"bsonType": "long"},
                "name": {"bsonType": "string"},
                "url": {"bsonType": "string"},
            },
        }
    }

    create_collection(
        db,
        "users",
        users_validator,
        "user_id",
        unique_index=True,
    )
    create_collection(
        db,
        "repositories",
        repositories_validator,
        "repo_id",
        unique_index=True,
    )


def validate_mongo_schema(db):
    collections = set(db.list_collection_names())
    expected_collections = {"users", "repositories"}
    missing_collections = expected_collections - collections

    print(f"------MongoDB collections: {collections}------")
    if missing_collections:
        raise Exception(
            f"--------MongoDB collections do not exist: {missing_collections}--------"
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
