from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from typing import Dict

from databases.mysql_connect import MySQLConnect
from databases.schema_manager import create_mysql_schema

class SparkWriteDatabase:
    def __init__(self, spark: SparkSession, db_config: Dict):
        self.spark = spark
        self.db_config = db_config
        
    def spark_write_mysql(self, df: DataFrame, table_name: str, jdbc_url: str, config: Dict, mode: str = "append"):
        #python add column temp
        spark_write_id = None
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

                # 4. Tạo ID cho lần chạy hiện tại
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
    
    def validate_mysql_write(
        self,
        table_name: str,
        jdbc_url: str,
        config: Dict,
        df: DataFrame,
        spark_write_id: str
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

        # Chỉ lấy các cột giống DataFrame gốc
        read_df = read_df.select(*df.columns)

        # 1. Kiểm tra số lượng records
        source_count = df.count()
        target_count = read_df.count()

        print(f"------Source count: {source_count}------")
        print(f"------MySQL count : {target_count}------")

        # 2. Tìm record thiếu trong MySQL
        missing_df = df.exceptAll(read_df)

        # 3. Tìm record dư / sai trong MySQL
        extra_df = read_df.exceptAll(df)

        missing_count = missing_df.count()
        extra_count = extra_df.count()
        if (
            source_count == target_count
            and missing_count == 0
            and extra_count == 0
        ):
            print(
                f"------Validation successful for {table_name}, {spark_write_id}------"
            )
            return True

        print(
            f"--------Validation failed for {table_name}, {spark_write_id}--------"
        )

        print(f"Missing records: {missing_count}")
        print(f"Extra/Wrong records: {extra_count}")

        if missing_count > 0:
            print("------Missing records------")
            missing_df.show(20, truncate=False)

        if extra_count > 0:
            print("------Extra/Wrong records------")
            extra_df.show(20, truncate=False)
        return False
        
    def spark_write_mongo(self, df: DataFrame, collection_name: str,
                        mongo_uri: str,
                        database_name: str,
                        mode: str = "append"
    ):
        df.write \
            .format("mongodb") \
            .option("connection.uri", mongo_uri) \
            .option("collection", collection_name) \
            .option("database", database_name) \
            .mode(mode).save()

        print(f"------Data written to MongoDB collection '{collection_name}' successfully------")
        
    def spark_write_all(self, df: DataFrame, mode: str = "append"):
        mysql_config = self.db_config["mysql"]
        mongo_config = self.db_config["mongo"]

        # 1. Write MySQL và lấy ID của lần write
        spark_write_id = self.spark_write_mysql(
            df=df,
            table_name=mysql_config["table"],
            jdbc_url=mysql_config["jdbc_url"],
            config=mysql_config["config"],
            mode=mode
        )

        # 2. Validate đúng lần write vừa rồi
        self.validate_mysql_write(
            table_name=mysql_config["table"],
            jdbc_url=mysql_config["jdbc_url"],
            config=mysql_config["config"],
            df=df,
            spark_write_id=spark_write_id
        )

        # 3. Write Mongo
        self.spark_write_mongo(
            df=df,
            collection_name=mongo_config["collection"],
            mongo_uri=mongo_config["uri"],
            database_name=mongo_config["database"],
            mode=mode
        )