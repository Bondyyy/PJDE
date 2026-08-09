from pyspark.sql import SparkSession, DataFrame
from typing import Dict

class SparkWriteDatabase:
    def __init__(self, spark: SparkSession, db_config: Dict):
        self.spark = spark
        self.db_config = db_config
        
    def spark_write_mysql(self, df: DataFrame, table_name: str, jdbc_url: str, config: Dict, mode: str = "append"):
        df.write.format("jdbc").option("url", jdbc_url).\
            option("dbtable", table_name).\
            option("user", config["user"]).\
            option("password", config["password"]).\
            option("driver", "com.mysql.cj.jdbc.Driver").\
            mode(mode).save()
        
