from bson.int64 import Int64
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType
import os

from config.database_config import (
    get_database_config,
    get_kafka_config,
)

CHECKPOINT_LOCATION = os.getenv( "SPARK_CHECKPOINT_LOCATION")
USER_COLLECTION = "users"
REPOSITORY_COLLECTION = "repositories"

def create_user_document(row):
    return {
        "user_id": Int64(row["user_id"]),
        "login": row["login"],
        "gravatar_id": row["gravatar_id"],
        "url": row["url"],
        "avatar_url": row["avatar_url"],
    }


def create_repository_document(row):
    return {
        "repo_id": Int64(row["repo_id"]),
        "name": row["name"],
        "url": row["url"],
    }


def apply_cdc_event(collection, row, primary_key, document):
    key_value = document[primary_key]

    """ nếu state là INSERT, lưu document vào collection, 
    nếu state là UPDATE, cập nhật document trong collection, 
    nếu state là DELETE, xóa document khỏi collection """
    
    if row["state"] == "INSERT":
        document_to_save = {
            key: value
            for key, value in document.items()
            if value is not None
        }
        collection.replace_one(
            {primary_key: key_value},
            document_to_save,
            upsert=True,
        )
    elif row["state"] == "UPDATE":
        fields_to_set = {
            key: value
            for key, value in document.items()
            if value is not None
        }
        fields_to_unset = {
            key: ""
            for key, value in document.items()
            if key != primary_key and value is None
        }

        update_operation = {"$set": fields_to_set}
        if fields_to_unset:
            update_operation["$unset"] = fields_to_unset

        collection.update_one(
            {primary_key: key_value},
            update_operation,
            upsert=True,
        )
    elif row["state"] == "DELETE":
        collection.delete_one({primary_key: key_value})
    else:
        raise ValueError(f"Unsupported CDC state: {row['state']}")


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    """
        Xử lý từng batch dữ liệu từ Kafka, 
        lọc theo entity, 
        sắp xếp theo log_id, và áp dụng các sự kiện INSERT, UPDATE, DELETE vào MongoDB.
        Sau khi xử lý xong, in ra số lượng các sự kiện đã được áp dụng
    """
    
    users_df = batch_df.filter(col("entity") == "users")
    repositories_df = batch_df.filter(col("entity") == "repositories")

    ordered_users = users_df.orderBy("log_id")
    ordered_repositories = repositories_df.orderBy("log_id")

    mongo_config = configDatabase["mongo"]
    counts = {
        "users": {"INSERT": 0, "UPDATE": 0, "DELETE": 0},
        "repositories": {"INSERT": 0, "UPDATE": 0, "DELETE": 0},
    }

    with MongoClient(mongo_config.uri) as client:
        db = client[mongo_config.db_name]
        users_collection = db[USER_COLLECTION]
        repositories_collection = db[REPOSITORY_COLLECTION]

        for row in ordered_users.toLocalIterator():
            document = create_user_document(row)
            apply_cdc_event(
                users_collection,
                row,
                "user_id",
                document,
            )
            counts["users"][row["state"]] += 1

        for row in ordered_repositories.toLocalIterator():
            document = create_repository_document(row)
            apply_cdc_event(
                repositories_collection,
                row,
                "repo_id",
                document,
            )
            counts["repositories"][row["state"]] += 1

    print(
        f"Batch {batch_id}: "
        f"users={counts['users']}, "
        f"repositories={counts['repositories']}"
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

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", configKafka["bootstrap_servers"]) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "BondyPJDE_validated") \
    .load()
    
kafka_schema = StructType([
    StructField("entity", StringType(), False),
    StructField("log_id", LongType(), False),
    StructField("user_id", LongType(), True),
    StructField("login", StringType(), True),
    StructField("gravatar_id", StringType(), True),
    StructField("avatar_url", StringType(), True),
    StructField("repo_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("url", StringType(), True),
    StructField("state", StringType(), True),
    StructField("log_timestamp", StringType(), True)
])

data_decode = df.select(col("value").cast("string"))
data = data_decode.select(from_json(col("value"), kafka_schema).alias("data")).select("data.*")
                        
data.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start() \
    .awaitTermination()
