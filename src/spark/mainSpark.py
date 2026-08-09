from config.spark_config import SparkConnect
from pyspark.sql.types import StructType, StructField, StringType, LongType

def main():
    sparkConnect = SparkConnect(app_name="MySparkApp", master_url="local[*]", 
                                executor_memory="4g", executor_cores=2, 
                                driver_memory="2g", num_executors=1, 
                                jar_packages=None, log_level="DEBUG")
    
    schema = StructType([
        StructField('users', StructType([
            StructField('user_id', LongType(), True),
            StructField('login', StringType(), True),
            StructField('gravatar_id', StringType(), True),
            StructField('url', StringType(), True),
            StructField('avatar_url', StringType(), True)
        ]), True),
        StructField('repositories', StructType([
            StructField('repo_id', LongType(), True),
            StructField('name', StringType(), True),
            StructField('url', StringType(), True)
        ]), True)
    ])
    
    df = sparkConnect.spark.read.json("D:\\ProjectDE\\data\\2015-03-01-17.json", schema=schema)
    df.show()

if __name__ == "__main__":
    main()