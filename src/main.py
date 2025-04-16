import sys
from src.utils.logging_utils import setup_logging, get_logger
from src.utils.config_utils import load_config, get_spark_config
from src.utils.spark_utils import create_spark_session, stop_spark_session
from src.jobs.data_processing import DataProcessingJob

def main():
    print("🔄 Processing data...")
    # Setup logging
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting PySpark application")
        
        # Load configuration
        config = load_config()
        spark_config = get_spark_config(config)
        
        # Create Spark session
        spark = create_spark_session(
            app_name=spark_config['app_name'],
            master=spark_config['master'],
            config=spark_config.get('config')
        )
        
        # Initialize and run job
        job = DataProcessingJob(spark, logger, config)
        success = job.run()
        
        if not success:
            logger.error("Job failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Application failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        # Stop Spark session if it exists
        if 'spark' in locals():
            stop_spark_session(spark)
        logger.info("PySpark application finished")

if __name__ == "__main__":
    main()