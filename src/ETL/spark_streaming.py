from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

from config.database_config import get_kafka_config

spark = (
    SparkSession.builder
    .appName("MySparkApp")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,"
        "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0"
    )
    .getOrCreate()
)

configKafka = get_kafka_config()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", configKafka["bootstrap_servers"]) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "BondyPJDE") \
    .load()
    
kafka_schema = StructType([
    StructField("log_id", LongType(), False),
    StructField("user_id", LongType(), True),
    StructField("login", StringType(), True),
    StructField("gravatar_id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("avatar_url", StringType(), True),
    StructField("state", StringType(), True),
    StructField("log_timestamp", StringType(), True)
])

data_decode = df.select(col("value").cast("string"))
data = data_decode.select(from_json(col("value"), kafka_schema).alias("data")).select("data.*")
                        

data.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "False") \
    .start() \
    .awaitTermination()
    