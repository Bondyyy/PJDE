from config.spark_config import SparkConnect
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)
from config.database_config import get_spark_config
from src.spark.spark_wrtie_data import SparkWriteDatabase
from pyspark.sql.functions import col

def main():

    jars = [
        "com.mysql:mysql-connector-j:8.0.33",
        "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0"
    ]

    spark_connect = SparkConnect(
        app_name="MySparkApp",
        master_url="local[*]",
        executor_memory="4g",
        executor_cores=2,
        driver_memory="2g",
        num_executors=1,
        jar_packages=jars,
        log_level="INFO"
    )

    schema = StructType([
        StructField("actor", StructType([
            StructField("id", LongType(), True),
            StructField("login", StringType(), True),
            StructField("gravatar_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("avatar_url", StringType(), True)
        ]), True),

        StructField("repo", StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True)
        ]), True)
    ])

    df = spark_connect.spark.read.json(
        r"D:\ProjectDE\data\2015-03-01-17.json",
        schema=schema
    )

    users_df = df.select(
        col("actor.id").alias("user_id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
        col("actor.avatar_url").alias("avatar_url")
    )

    spark_config = get_spark_config()

    writer = SparkWriteDatabase(
        spark_connect.spark,
        spark_config
    )

    # WRITE
    (
        spark_id,
        mysql_expected_df,
        mongo_expected_df
    ) = writer.spark_write_all(
        df=users_df,
        primary_key="user_id"
    )

    writer.validate_all(
        mysql_expected_df=mysql_expected_df,
        mongo_expected_df=mongo_expected_df,
        spark_write_id=spark_id
    )


if __name__ == "__main__":
    main()