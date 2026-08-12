from bson.int64 import Int64
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

from config.database_config import (
    get_database_config,
    get_kafka_config,
    get_spark_config,
)

def create_user_document(row):
    # Tạo document từ row của DF và loại bỏ các trường không cần thiết
    document = row.asDict()
    document.pop("log_id")
    document.pop("state")
    document.pop("log_timestamp")
    document["user_id"] = Int64(document["user_id"])
    return document


def process_batch(batch_df, batch_id):
    # Nếu batch_df rỗng, không làm gì 
    if batch_df.isEmpty():
        return

    # Lọc các dòng theo trạng thái
    insert_df = batch_df.filter(col("state") == "INSERT")
    update_df = batch_df.filter(col("state") == "UPDATE")
    delete_df = batch_df.filter(col("state") == "DELETE")

    mongo_config = configDatabase["mongo"]
    collection_name = configSpark["mongo"]["collection"]

    with MongoClient(mongo_config.uri) as client:
        collection = client[mongo_config.db_name][collection_name]

        # Xử lý các dòng INSERT
        insert_count = 0
        for row in insert_df.toLocalIterator():
            document = create_user_document(row)
            document = {
                key: value
                for key, value in document.items()
                if value is not None
            }

            collection.replace_one(
                {"user_id": document["user_id"]},
                document,
                upsert=True, # Trùng thì update, không trùng thì insert
            )
            insert_count += 1

        # Xử lý các dòng UPDATE
        update_count = 0
        for row in update_df.toLocalIterator():
            document = create_user_document(row)
            user_id = document["user_id"]
            # Tạo các trường để set và unset trong update operation
            fields_to_set = {
                key: value
                for key, value in document.items()
                if value is not None
            }
            fields_to_unset = {
                key: ""
                for key, value in document.items()
                if value is None
            }

            update_operation = {"$set": fields_to_set}
            if fields_to_unset:
                update_operation["$unset"] = fields_to_unset

            collection.update_one(
                {"user_id": user_id},
                update_operation,
                upsert=True,
            )
            update_count += 1

        # Xử lý các dòng DELETE
        delete_count = 0
        for row in delete_df.toLocalIterator():
            collection.delete_one({"user_id": Int64(row["user_id"])})
            delete_count += 1

    print(
        f"Batch {batch_id}: "
        f"INSERT={insert_count}, UPDATE={update_count}, DELETE={delete_count}"
    )

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
configDatabase = get_database_config()
configSpark = get_spark_config()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", configKafka["bootstrap_servers"]) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "BondyPJDE_validated") \
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
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "D:/ProjectDE/checkpoints/spark_mongo") \
    .start() \
    .awaitTermination()
