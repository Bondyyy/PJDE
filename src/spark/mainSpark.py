import os
from copy import deepcopy

from dotenv import load_dotenv

from config.spark_config import SparkConnect
from config.database_config import get_spark_config
from src.spark.spark_wrtie_data import SparkWriteDatabase

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

from pyspark.sql.functions import col

load_dotenv()

SOURCE_DATA_PATH = os.getenv("SOURCE_DATA_PATH")

def create_mysql_writer(spark, base_config, table_name):
    """
    Tạo một writer riêng cho từng MySQL table.
    """
    config = deepcopy(base_config)
    config["mysql"]["table"] = table_name
    return SparkWriteDatabase(spark,config)

def write_and_validate_mysql(writer,df):
    """
    Write dữ liệu vào MySQL,
    sau đó validate kết quả write.
    """
    (
        spark_write_id,
        expected_df
    ) = writer.spark_write_mysql_pk(
        df=df,
        mode="append"
    )
    writer.validate_mysql_write(
        expected_df=expected_df,
        spark_write_id=spark_write_id
    )

def main():
    jars = [
        "com.mysql:mysql-connector-j:8.0.33"
    ]

    spark_connect = SparkConnect(app_name="PJDEInitialLoad", master_url="local[*]", executor_memory="4g", executor_cores=2,
        driver_memory="2g", num_executors=1, jar_packages=jars, log_level="INFO")

    try:
        schema = StructType([
            StructField("actor", StructType([
                StructField("id", LongType(), True),
                StructField("login", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("avatar_url", StringType(), True),
            ]), True),

            StructField("repo", StructType([
                StructField("id", LongType(), True),
                StructField("name", StringType(), True),
                StructField("url", StringType(), True),
            ]), True),
        ])

        print(f"------Reading source: {SOURCE_DATA_PATH}------")

        df = spark_connect.spark.read.json(
                SOURCE_DATA_PATH,
                schema=schema
            )

        # USERS
        users_df = df.select(
            col("actor.id").alias("user_id"), col("actor.login").alias("login"),
            col("actor.gravatar_id").alias("gravatar_id"), col("actor.url").alias("url"),
            col("actor.avatar_url").alias("avatar_url")
        )
        # REPOSITORIES
        repositories_df = df.select(
            col("repo.id").alias("repo_id"),
            col("repo.name").alias("name"),
            col("repo.url").alias("url")
        )

        base_config = get_spark_config()

        users_writer = create_mysql_writer(spark_connect.spark, base_config, "users")
        repositories_writer = create_mysql_writer(spark_connect.spark, base_config, "repositories")

        print("------Starting users initial load------")
        write_and_validate_mysql(users_writer, users_df)

        print("------Starting repositories initial load------")
        write_and_validate_mysql(repositories_writer, repositories_df)

        print("------Initial load completed successfully------")

    finally:
        spark_connect.stop()

if __name__ == "__main__":
    main()