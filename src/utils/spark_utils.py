from pyspark.sql import SparkSession

def create_spark_session(app_name, master="local[*]", config=None):
    """Create and return a Spark session with the given configuration"""
    spark_builder = SparkSession.builder.appName(app_name).master(master)
    
    if config:
        for key, value in config.items():
            spark_builder.config(key, value)
    
    return spark_builder.getOrCreate()

def stop_spark_session(spark):
    """Stop the Spark session"""
    spark.stop()