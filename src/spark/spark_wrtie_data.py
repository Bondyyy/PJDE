from pymongo import MongoClient
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col
from typing import Dict

from databases.mysql_connect import MySQLConnect


class SparkWriteDatabase:
    def __init__(self, spark: SparkSession, db_config: Dict):
        self.spark = spark
        self.db_config = db_config

    def spark_write_mysql(self, df: DataFrame, spark_write_id: str = None, mode: str = "append"):
        """
        Write DataFrame xuống MySQL.

        - Tự tạo cột spark_write nếu chưa có.
        - Nếu không truyền spark_write_id:
            tạo spark_1, spark_2, ...
        - Nếu truyền spark_write_id:
            dùng lại ID đó, phục vụ ghi bù.
        """
        mysql_config = self.db_config["mysql"]
        table_name = mysql_config["table"]
        jdbc_url = mysql_config["jdbc_url"]
        config = mysql_config["config"]
        try:
            with MySQLConnect(config["host"], config["port"],
                config["user"], config["password"]
            ) as mysql_conn:
                connection = mysql_conn.connection
                cursor = mysql_conn.cursor
                database_name = config["database"]

                cursor.execute(f"USE `{database_name}`")
                
                # Kiểm tra cột spark_write
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                      AND TABLE_NAME = %s
                      AND COLUMN_NAME = 'spark_write'
                """, (database_name, table_name))
                column_exists = cursor.fetchone()[0] > 0

                # Chưa có thì tạo
                if not column_exists:
                    cursor.execute(f"""
                        ALTER TABLE `{table_name}`
                        ADD COLUMN spark_write VARCHAR(50) NULL
                    """)
                    connection.commit()
                    
                    print(
                        f"------Added spark_write column to '{table_name}'------"
                    )

                # Tạo ID mới nếu đây là write mới
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
                    spark_write_id = f"spark_{last_spark_id + 1}"

        except Exception as e:
            raise Exception(
                f"--------Error while preparing MySQL table '{table_name}': {e}--------"
            ) from e

        # Thêm ID của lần write vào DataFrame
        df_write = df.withColumn("spark_write", lit(spark_write_id))

        # Spark write MySQL
        (
            df_write.write
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", table_name)
                .option("user", config["user"])
                .option("password", config["password"])
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .mode(mode)
                .save()
        )

        print(f"------Data written to MySQL table '{table_name}' successfully------")
        return spark_write_id

    def spark_write_mongo(
        self,
        df: DataFrame,
        spark_write_id: str,
        mode: str = "append"
    ):
        """
        Write DataFrame xuống MongoDB
        với cùng spark_write_id của MySQL.
        """

        mongo_config = self.db_config["mongo"]
        collection_name = mongo_config["collection"]
        mongo_uri = mongo_config["uri"]
        database_name = mongo_config["database"]
        df_write = df.withColumn("spark_write", lit(spark_write_id))

        (
            df_write.write
            .format("mongodb")
            .option("connection.uri", mongo_uri)
            .option("collection", collection_name)
            .option("database", database_name)
            .mode(mode)
            .save()
        )

        print(f"------Data written to MongoDB collection '{collection_name}' successfully------")

    def get_mysql_primary_key(self):
        """
        Lấy danh sách PRIMARY KEY của MySQL table.
        """

        mysql_config = self.db_config["mysql"]
        table_name = mysql_config["table"]
        config = mysql_config["config"]

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
            return [row[0] for row in cursor.fetchall()]

    def has_mongo_unique_index(self, primary_key: str):
        """
        Kiểm tra field MongoDB có UNIQUE index không.
        """

        mongo_config = self.db_config["mongo"]
        collection_name = mongo_config["collection"]
        mongo_uri = mongo_config["uri"]
        database_name = mongo_config["database"]
        client = MongoClient(mongo_uri)

        try:
            collection = client[database_name][collection_name]
            # MongoDB _id luôn unique
            if primary_key == "_id":
                return True
            for index in collection.list_indexes():
                index_keys = list(index["key"].keys())
                if index.get("unique", False) and index_keys == [primary_key]:
                    return True
            return False
        finally:
            client.close()

    def spark_write_mysql_pk(
        self,
        df: DataFrame,
        mode: str = "append"
    ):
        """
        Nếu table KHÔNG có PK:
            -> write bình thường.

        Nếu table CÓ PK:
            -> lấy các PK hiện có
            -> left anti join
            -> ghi record mới.
        """
        mysql_config = self.db_config["mysql"]
        table_name = mysql_config["table"]
        jdbc_url = mysql_config["jdbc_url"]
        config = mysql_config["config"]

        primary_keys = self.get_mysql_primary_key()

        # Không có PK -> normal write
        if not primary_keys:
            print(
                f"------Table '{table_name}' has no PRIMARY KEY. "
                f"Using normal Spark write------"
            )

            spark_write_id = self.spark_write_mysql(df=df, mode=mode)
            return spark_write_id, df
        print(f"------Table '{table_name}' PRIMARY KEY: {primary_keys}------")

        # Kiểm tra PK có trong DataFrame kh
        for pk in primary_keys:
            if pk not in df.columns:
                raise Exception(
                    f"Primary key '{pk}' "
                    f"does not exist in Spark DataFrame"
                )
        # Đọc các PK từ MySQL
        pk_columns = ", ".join(f"`{pk}`" for pk in primary_keys)
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

        df_unique = df.dropDuplicates(primary_keys)

        #  giữ record chưa tồn tại
        new_df = (
            df_unique
            .join(existing_df, on=primary_keys, how="left_anti")
            .cache()
        )
        new_count = new_df.count()
        print(f"------MySQL new records to write: {new_count}------")

        spark_write_id = self.spark_write_mysql(df=new_df, mode=mode)
        return spark_write_id, new_df

    def spark_write_mongo_pk(self, df: DataFrame, primary_key: str, 
                             spark_write_id: str, mode: str = "append"):
        """
        Nếu primary_key kh unique trong Mongo:
            -> write bình thường.

        Nếu primary_key có unique index:
            -> left anti join
            -> chỉ ghi record mới.
        """
        mongo_config = self.db_config["mongo"]
        collection_name = mongo_config["collection"]
        mongo_uri = mongo_config["uri"]
        database_name = mongo_config["database"]
        has_unique = self.has_mongo_unique_index(primary_key)

        # Không unique -> normal write
        if not has_unique:
            print(f"------MongoDB field '{primary_key}' is not UNIQUE. Using normal Spark write------")
            self.spark_write_mongo(
                df=df,
                spark_write_id=spark_write_id,
                mode=mode
            )
            return df

        print(f"------MongoDB UNIQUE key: '{primary_key}'------")   

        if primary_key not in df.columns:
            raise Exception(f"Primary key '{primary_key}' does not exist in Spark DataFrame")

        # Đọc những key đã có
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
        # Source chỉ giữ một record 
        df_unique = df.dropDuplicates([primary_key])
        # Chỉ lấy record chưa tồn tại
        new_df = (
            df_unique
            .join(existing_df, on=primary_key, how="left_anti")
            .cache()
        )
        new_count = new_df.count()
        print(f"------MongoDB new records to write: {new_count}------")
        
        self.spark_write_mongo(
            df=new_df,
            spark_write_id=spark_write_id,
            mode=mode
        )
        return new_df

    def spark_write_all(self, df: DataFrame,
            primary_key: str, mode: str = "append"
    ):
        """
        Điều phối write cho MySQL + MongoDB.

        Hai database tự quyết định:
            - không có PK -> normal write
            - có PK -> anti join trước khi write
        """
        # MySQL
        spark_write_id, mysql_expected_df = self.spark_write_mysql_pk(
            df=df,
            mode=mode
        )

        # MongoDB
        mongo_expected_df = self.spark_write_mongo_pk(
            df=df, primary_key=primary_key,
            spark_write_id=spark_write_id, mode=mode
        )
        return spark_write_id, mysql_expected_df, mongo_expected_df

    def compare_write_data(
        self,
        source_df: DataFrame,
        target_df: DataFrame,
        database_name: str
    ):
        """
        So sánh dữ liệu expected với dữ liệu đọc lại DB.

        exceptAll được dùng để giữ đúng multiplicity
        của duplicate record.
        """

        # Chỉ giữ các column của source
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

    def validate_mysql_write(
        self,
        expected_df: DataFrame,
        spark_write_id: str,
        retry: int = 0,
        max_retry: int = 3
    ):
        """
        Validate MySQL.

        Nếu chỉ thiếu record:
            -> ghi bù
            -> validate lại.

        Nếu có extra:
            -> báo lỗi, không tự sửa.
        """

        mysql_config = self.db_config["mysql"]
        table_name = mysql_config["table"]
        jdbc_url = mysql_config["jdbc_url"]
        config = mysql_config["config"]

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

        (
            success,
            missing_df,
            extra_df,
            missing_count,
            extra_count
        ) = self.compare_write_data(
            source_df=expected_df,
            target_df=read_df,
            database_name="MySQL"
        )

        if success:
            print(
                f"------Validation successful for MySQL {table_name}, {spark_write_id}------"
            )
            return True

        print(
            f"--------Validation failed for MySQL {table_name}, {spark_write_id}--------"
        )

        # Chỉ thiếu -> ghi bù
        if (
            missing_count > 0
            and extra_count == 0
            and retry < max_retry
        ):
            print("------Missing MySQL records------")
            missing_df.show(20, truncate=False)

            self.spark_write_mysql(
                df=missing_df,
                spark_write_id=spark_write_id,
                mode="append"
            )

            return self.validate_mysql_write(
                expected_df=expected_df,
                spark_write_id=spark_write_id,
                retry=retry + 1,
                max_retry=max_retry
            )

        if extra_count > 0:
            print("------Extra MySQL records------")
            extra_df.show(20, truncate=False)

        return False

    def validate_mongo_write(
        self,
        expected_df: DataFrame,
        spark_write_id: str,
        retry: int = 0,
        max_retry: int = 3
    ):
        """
        Validate MongoDB.

        Nếu chỉ thiếu:
            -> ghi bù
            -> validate lại.
        """

        mongo_config = self.db_config["mongo"]
        collection_name = mongo_config["collection"]
        mongo_uri = mongo_config["uri"]
        database_name = mongo_config["database"]

        read_df = (
            self.spark.read
                .format("mongodb")
                .option("connection.uri", mongo_uri)
                .option("collection", collection_name)
                .option("database", database_name)
                .load()
                .filter(col("spark_write") == spark_write_id)
        )

        (
            success,
            missing_df,
            extra_df,
            missing_count,
            extra_count
        ) = self.compare_write_data(
            source_df=expected_df,
            target_df=read_df,
            database_name="MongoDB"
        )

        if success:
            print(f"------Validation successful for MongoDB {collection_name}, {spark_write_id}------")
            return True
        print(f"--------Validation failed for MongoDB {collection_name}, {spark_write_id}--------")

        # Chỉ thiếu -> ghi bù
        if (
            missing_count > 0 and extra_count == 0 and retry < max_retry
        ):
            print("------Missing MongoDB records------")
            missing_df.show(20, truncate=False)

            self.spark_write_mongo(
                df=missing_df,
                spark_write_id=spark_write_id,
                mode="append"
            )

            return self.validate_mongo_write(
                expected_df=expected_df,
                spark_write_id=spark_write_id,
                retry=retry + 1,
                max_retry=max_retry
            )

        if extra_count > 0:
            print("------Extra/Wrong MongoDB records------")
            extra_df.show(20, truncate=False)

        return False


    def validate_all(
        self,
        mysql_expected_df: DataFrame,
        mongo_expected_df: DataFrame,
        spark_write_id: str
    ):
        """
        Validate cả MySQL + MongoDB.

        MySQL và Mongo có expected_df riêng vì
        constraint của hai DB có thể khác nhau.
        """
        try:
            mysql_result = self.validate_mysql_write(
                expected_df=mysql_expected_df,
                spark_write_id=spark_write_id
            )

            mongo_result = self.validate_mongo_write(
                expected_df=mongo_expected_df,
                spark_write_id=spark_write_id
            )

            if mysql_result and mongo_result:
                print(
                    f"------Validation successful "
                    f"for all databases ({spark_write_id})------"
                )
                return True

            print(
                f"--------Validation failed for one or more databases "
                f"({spark_write_id})--------"
            )
            return False

        finally:
            mysql_expected_df.unpersist()
            mongo_expected_df.unpersist()