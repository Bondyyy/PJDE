from config.spark_config import SparkConnect
from pyspark.sql.types import StructType, StructField, StringType, LongType
from config.database_config import get_spark_config
from src.spark.spark_wrtie_data import SparkWriteDatabase
from pyspark.sql.functions import col

def main():
    jars = [
        "com.mysql:mysql-connector-j:8.0.33"
    ]
    sparkConnect = SparkConnect(app_name="MySparkApp", master_url="local[*]", 
                                executor_memory="4g", executor_cores=2, 
                                driver_memory="2g", num_executors=1, 
                                jar_packages=jars, log_level="DEBUG")
    
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
    
    df = sparkConnect.spark.read.json("D:\\ProjectDE\\data\\2015-03-01-17.json", schema=schema)
    df_write_table = df.select(
        col("actor.id").alias("user_id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
        col("actor.avatar_url").alias("avatar_url")
    )
    spark_config = get_spark_config()
    df_write = SparkWriteDatabase(sparkConnect.spark, spark_config)
    df_write.spark_write_mysql(df_write_table, spark_config["mysql"]["table"],
                               spark_config["mysql"]["jdbc_url"], spark_config["mysql"])

if __name__ == "__main__":
    main()