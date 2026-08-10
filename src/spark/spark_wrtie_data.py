from pymongo import MongoClient
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col
from typing import Dict

from databases.mysql_connect import MySQLConnect
from databases.schema_manager import create_mysql_schema

class SparkWriteDatabase:
    def __init__(self, spark: SparkSession, db_config: Dict):
        self.spark = spark
        self.db_config = db_config
        
    def spark_write_mysql(self, df: DataFrame, table_name: str, jdbc_url: str, config: Dict, spark_write_id: str =None, mode: str = "append"):
        #python add column temp
        try:
            with MySQLConnect(config["host"], config["port"], config["user"], config["password"]) as mysql_conn:
                connection = mysql_conn.connection
                cursor = mysql_conn.cursor
                database_name = config["database"]
                cursor.execute(f"USE `{database_name}`")

                # 1. Kiểm tra cột spark_write đã tồn tại chưa
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND COLUMN_NAME = 'spark_write'
                """, (database_name, table_name))
                column_exists = cursor.fetchone()[0] > 0

                # 2. Nếu chưa có thì tạo
                if not column_exists:
                    cursor.execute(f"""
                        ALTER TABLE `{table_name}`
                        ADD COLUMN spark_write VARCHAR(50) NULL
                    """)
                    connection.commit()
                    print(
                        f"------Added spark_write column "
                        f"to '{table_name}'------"
                    )

                # 3. Tìm số Spark run lớn nhất
                if spark_write_id is None:
                    cursor.execute(f"""
                        SELECT COALESCE(
                            MAX(
                                CAST(
                                    SUBSTRING_INDEX(spark_write, '_', -1)
                                    AS UNSIGNED
                                )
                            ),
                            0
                        )
                        FROM `{table_name}`
                        WHERE spark_write LIKE 'spark_%'
                    """)
                    last_spark_id = cursor.fetchone()[0]

                    next_spark_id = last_spark_id + 1
                    spark_write_id = f"spark_{next_spark_id}"


                connection.commit()
        except Exception as e:
            raise Exception(
                f"--------Error while preparing MySQL "
                f"table '{table_name}': {e}--------"
            ) from e
        
        #spark write
        df_write = df.withColumn("spark_write", lit(spark_write_id)) 
        
        df_write.write.format("jdbc").option("url", jdbc_url).\
            option("dbtable", table_name).\
            option("user", config["user"]).\
            option("password", config["password"]).\
            option("driver", "com.mysql.cj.jdbc.Driver").\
            mode(mode).save()
        print(f"------Data written to MySQL table '{table_name}' successfully------")
        return spark_write_id
    
    def spark_write_mongo(
            self,
            df: DataFrame,
            collection_name: str,
            mongo_uri: str,
            database_name: str,
            spark_write_id: str,
            mode: str = "append"
        ):
            df_write = df.withColumn(
                "spark_write",
                lit(spark_write_id)
            )
    
            df_write.write \
                .format("mongodb") \
                .option("connection.uri", mongo_uri) \
                .option("collection", collection_name) \
                .option("database", database_name) \
                .mode(mode) \
                .save()
    
            print(
                f"------Data written to MongoDB collection "
                f"'{collection_name}' successfully------"
            )
    
    def validate_mysql_write(
        self,
        table_name: str,
        jdbc_url: str,
        config: Dict,
        df: DataFrame,
        spark_write_id: str,
        retry: int = 0,
        max_retry: int = 3
    ):
        query = f"""
            (
                SELECT *
                FROM `{table_name}`
                WHERE spark_write = '{spark_write_id}'
            ) AS current_spark_write
        """

        read_df = (
            self.spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", query)
            .option("user", config["user"])
            .option("password", config["password"])
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )

        success, missing_df, extra_df, missing_count, extra_count = \
            self.compare_write_data(
                source_df=df,
                target_df=read_df,
                database_name="MySQL",
                spark_write_id=spark_write_id
            )

        if success:
            print(
                f"------Validation successful for MySQL "
                f"{table_name}, {spark_write_id}------"
            )
            return True

        print(
            f"--------Validation failed for MySQL "
            f"{table_name}, {spark_write_id}--------"
        )

        if (
            missing_count > 0
            and extra_count == 0
            and retry < max_retry
        ):
            print("------Missing MySQL records------")
            missing_df.show(20, truncate=False)

            self.spark_write_mysql(
                df=missing_df,
                table_name=table_name,
                jdbc_url=jdbc_url,
                config=config,
                spark_write_id=spark_write_id,
                mode="append"
            )

            return self.validate_mysql_write(
                table_name=table_name,
                jdbc_url=jdbc_url,
                config=config,
                df=df,
                spark_write_id=spark_write_id,
                retry=retry + 1,
                max_retry=max_retry
            )

        if extra_count > 0:
            print("------Extra/Wrong MySQL records------")
            extra_df.show(20, truncate=False)

        return False
    
    def validate_mongo_write(
        self,
        collection_name: str,
        mongo_uri: str,
        database_name: str,
        df: DataFrame,
        spark_write_id: str,
        retry: int = 0,
        max_retry: int = 3
    ):
        # 1. Đọc MongoDB
        read_df = (
            self.spark.read
            .format("mongodb")
            .option("connection.uri", mongo_uri)
            .option("collection", collection_name)
            .option("database", database_name)
            .load()
        )

        # 2. Chỉ lấy dữ liệu của lần Spark write hiện tại
        read_df = read_df.filter(
            col("spark_write") == spark_write_id
        )

        # 3. Dùng lại hàm compare chung
        success, missing_df, extra_df, missing_count, extra_count = \
            self.compare_write_data(
                source_df=df,
                target_df=read_df,
                database_name="MongoDB",
                spark_write_id=spark_write_id
            )

        if success:
            print(
                f"------Validation successful for MongoDB "
                f"{collection_name}, {spark_write_id}------"
            )
            return True

        print(
            f"--------Validation failed for MongoDB "
            f"{collection_name}, {spark_write_id}--------"
        )

        # 4. Nếu chỉ thiếu thì ghi bù
        if (
            missing_count > 0
            and extra_count == 0
            and retry < max_retry
        ):
            print("------Missing MongoDB records------")
            missing_df.show(20, truncate=False)

            self.spark_write_mongo(
                df=missing_df,
                collection_name=collection_name,
                mongo_uri=mongo_uri,
                database_name=database_name,
                spark_write_id=spark_write_id,
                mode="append"
            )

            return self.validate_mongo_write(
                collection_name=collection_name,
                mongo_uri=mongo_uri,
                database_name=database_name,
                df=df,
                spark_write_id=spark_write_id,
                retry=retry + 1,
                max_retry=max_retry
            )

        if extra_count > 0:
            print("------Extra/Wrong MongoDB records------")
            extra_df.show(20, truncate=False)

        return False
    
    def spark_write_mysql_pk(
        self,
        df: DataFrame,
        table_name: str,
        jdbc_url: str,
        config: Dict,
        mode: str = "append"
    ):
        # 1. Kiểm tra MySQL table có PRIMARY KEY không
        primary_keys = self.get_mysql_primary_key(
            table_name=table_name,
            config=config
        )

        # 2. Không có PK -> không cần join, write bình thường
        if not primary_keys:
            print(
                f"------Table '{table_name}' has no PRIMARY KEY. "
                f"Using normal Spark write------"
            )

            spark_write_id = self.spark_write_mysql(
                df=df,
                table_name=table_name,
                jdbc_url=jdbc_url,
                config=config,
                mode=mode
            )

            # expected_df chính là toàn bộ source
            return spark_write_id, df

        print(
            f"------Table '{table_name}' PRIMARY KEY: "
            f"{primary_keys}------"
        )

        # 3. Kiểm tra PK có tồn tại trong DataFrame không
        for pk in primary_keys:
            if pk not in df.columns:
                raise Exception(
                    f"Primary key '{pk}' does not exist "
                    f"in Spark DataFrame"
                )

        # 4. Chỉ đọc các PRIMARY KEY hiện có trong MySQL
        pk_columns = ", ".join(
            [f"`{pk}`" for pk in primary_keys]
        )

        query = f"""
            (
                SELECT {pk_columns}
                FROM `{table_name}`
            ) AS existing_keys
        """

        existing_df = (
            self.spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", query)
            .option("user", config["user"])
            .option("password", config["password"])
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )

        # 5. Vì bảng có PK -> source cũng không thể có PK trùng nhau
        df_unique = df.dropDuplicates(primary_keys)

        # 6. LEFT ANTI JOIN:
        # chỉ giữ record mà PK chưa tồn tại trong MySQL
        new_df = (
            df_unique.join(
                existing_df,
                on=primary_keys,
                how="left_anti"
            )
            .cache()
        )

        # Materialize cache trước khi DB bị thay đổi
        new_count = new_df.count()

        print(
            f"------MySQL new records to write: "
            f"{new_count}------"
        )

        # 7. Dùng lại hàm write cũ
        spark_write_id = self.spark_write_mysql(
            df=new_df,
            table_name=table_name,
            jdbc_url=jdbc_url,
            config=config,
            mode=mode
        )

        return spark_write_id, new_df
    
    def spark_write_mongo_pk(
        self,
        df: DataFrame,
        collection_name: str,
        mongo_uri: str,
        database_name: str,
        primary_key: str,
        spark_write_id: str,
        mode: str = "append"
    ):
        # 1. Kiểm tra field này có unique index 
        has_unique = self.has_mongo_unique_index(
            collection_name=collection_name,
            mongo_uri=mongo_uri,
            database_name=database_name,
            primary_key=primary_key
        )

        # 2. Không unique -> không cần anti join
        if not has_unique:
            print(
                f"------MongoDB field '{primary_key}' "
                f"is not UNIQUE. Using normal Spark write------"
            )

            self.spark_write_mongo(
                df=df,
                collection_name=collection_name,
                mongo_uri=mongo_uri,
                database_name=database_name,
                spark_write_id=spark_write_id,
                mode=mode
            )

            return df

        print(
            f"------MongoDB UNIQUE key: "
            f"{primary_key}------"
        )

        if primary_key not in df.columns:
            raise Exception(
                f"Primary key '{primary_key}' does not exist "
                f"in Spark DataFrame"
            )

        # 3. Đọc key đã tồn tại
        existing_df = (
            self.spark.read
            .format("mongodb")
            .option("connection.uri", mongo_uri)
            .option("collection", collection_name)
            .option("database", database_name)
            .load()
            .select(primary_key)
            .dropDuplicates()
        )

        # 4. Source cũng không được trùng unique key
        df_unique = df.dropDuplicates([primary_key])

        # 5. Chỉ lấy record mới
        new_df = (
            df_unique.join(
                existing_df,
                on=primary_key,
                how="left_anti"
            )
            .cache()
        )

        new_count = new_df.count()

        print(
            f"------MongoDB new records to write: "
            f"{new_count}------"
        )

        # 6. Dùng lại writer hiện có
        self.spark_write_mongo(
            df=new_df,
            collection_name=collection_name,
            mongo_uri=mongo_uri,
            database_name=database_name,
            spark_write_id=spark_write_id,
            mode=mode
        )

        return new_df
        
    def spark_write_all_pk(
        self,
        df: DataFrame,
        primary_key: str,
        mode: str = "append"
    ):
        mysql_config = self.db_config["mysql"]
        mongo_config = self.db_config["mongo"]

        # 1. MySQL
        # MySQL tự kiểm tra PRIMARY KEY trong schema
        spark_write_id, mysql_expected_df = self.spark_write_mysql_pk(
            df=df,
            table_name=mysql_config["table"],
            jdbc_url=mysql_config["jdbc_url"],
            config=mysql_config["config"],
            mode=mode
        )

        # 2. MongoDB
        # Mongo cần truyền field mà ta muốn xem như unique key
        mongo_expected_df = self.spark_write_mongo_pk(
            df=df,
            collection_name=mongo_config["collection"],
            mongo_uri=mongo_config["uri"],
            database_name=mongo_config["database"],
            primary_key=primary_key,
            spark_write_id=spark_write_id,
            mode=mode
        )

        # 3. Trả lại những gì cần cho validate
        return (
            spark_write_id,
            mysql_expected_df,
            mongo_expected_df
        )

    def compare_write_data(
            self,
            source_df: DataFrame,
            target_df: DataFrame,
            database_name: str,
            spark_write_id: str
        ):
            # Chỉ giữ các cột giống source
            target_df = target_df.select(*source_df.columns)
    
            source_count = source_df.count()
            target_count = target_df.count()
    
            missing_df = source_df.exceptAll(target_df)
            extra_df = target_df.exceptAll(source_df)
    
            missing_count = missing_df.count()
            extra_count = extra_df.count()
    
            print(f"------Source count : {source_count}------")
            print(f"------{database_name} count : {target_count}------")
            print(f"------Missing records: {missing_count}------")
            print(f"------Extra/Wrong records: {extra_count}------")
    
            success = (
                source_count == target_count
                and missing_count == 0
                and extra_count == 0
            )
            return success, missing_df, extra_df, missing_count, extra_count
    
    def validate_all_pk(
        self,
        mysql_expected_df: DataFrame,
        mongo_expected_df: DataFrame,
        spark_write_id: str
    ):
        mysql_config = self.db_config["mysql"]
        mongo_config = self.db_config["mongo"]

        mysql_result = self.validate_mysql_write(
            table_name=mysql_config["table"],
            jdbc_url=mysql_config["jdbc_url"],
            config=mysql_config["config"],
            df=mysql_expected_df,
            spark_write_id=spark_write_id
        )

        mongo_result = self.validate_mongo_write(
            collection_name=mongo_config["collection"],
            mongo_uri=mongo_config["uri"],
            database_name=mongo_config["database"],
            df=mongo_expected_df,
            spark_write_id=spark_write_id
        )

        # Giải phóng cache sau khi validate xong
        mysql_expected_df.unpersist()
        mongo_expected_df.unpersist()

        if mysql_result and mongo_result:
            print(
                f"------Validation successful "
                f"for all databases "
                f"({spark_write_id})------"
            )
            return True

        print(
            f"--------Validation failed "
            f"for one or more databases "
            f"({spark_write_id})--------"
        )

        return False
    
    def get_mysql_primary_key(
        self,
        table_name: str,
        config: Dict
    ):
        with MySQLConnect(
            config["host"],
            config["port"],
            config["user"],
            config["password"]
        ) as mysql_conn:

            cursor = mysql_conn.cursor
            database_name = config["database"]

            cursor.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
            """, (database_name, table_name))

            primary_keys = [
                row[0]
                for row in cursor.fetchall()
            ]

            return primary_keys
        
    def has_mongo_unique_index(
        self,
        collection_name: str,
        mongo_uri: str,
        database_name: str,
        primary_key: str
    ):
        client = MongoClient(mongo_uri)

        try:
            collection = client[database_name][collection_name]

            # _id luôn là unique identifier của MongoDB
            if primary_key == "_id":
                return True

            for index in collection.list_indexes():
                index_keys = list(index["key"].keys())

                if (
                    index.get("unique", False)
                    and index_keys == [primary_key]
                ):
                    return True

            return False

        finally:
            client.close()