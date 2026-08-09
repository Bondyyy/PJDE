from config.spark_config import SparkConnect

def main():
    sparkConnect = SparkConnect(app_name="MySparkApp", master_url="local[*]", 
                                executor_memory="4g", executor_cores=2, 
                                driver_memory="2g", num_executors=1, 
                                jar_packages=None, log_level="DEBUG")
    
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Name", "Age"]
    df = sparkConnect.spark.createDataFrame(data, columns)
    df.show()
    sparkConnect.stop()

if __name__ == "__main__":
    main()