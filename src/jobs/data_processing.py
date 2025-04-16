from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class DataProcessingJob:
    def __init__(self, spark, logger, config):
        self.spark = spark
        self.logger = logger
        self.config = config
        
    def read_data(self, file_path):
        """Read data from the given path"""
        self.logger.info(f"Reading data from {file_path}")
        return self.spark.read.csv(file_path, header=True, inferSchema=True)
    
    def process_data(self, df: DataFrame) -> DataFrame:
        """Process the input DataFrame"""
        self.logger.info("Processing data")
        
        # Example processing - count by category
        processed_df = df.groupBy("category").agg(
            F.count("*").alias("count"),
            F.avg("value").alias("avg_value")
        )
        
        return processed_df
    
    def write_data(self, df: DataFrame, output_path: str):
        """Write data to the given path"""
        self.logger.info(f"Writing data to {output_path}")
        df.write.mode("overwrite").parquet(output_path)
    
    def run(self):
        """Run the data processing job"""
        try:
            input_path = self.config['data']['input_path']
            output_path = self.config['data']['output_path']
            
            # Read data
            input_df = self.read_data(input_path)
            
            # Process data
            processed_df = self.process_data(input_df)
            
            # Write results
            self.write_data(processed_df, output_path)
            
            self.logger.info("Data processing job completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error in data processing job: {str(e)}", exc_info=True)
            return False