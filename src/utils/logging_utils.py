import logging
import logging.config
import yaml
import os
from pathlib import Path

def setup_logging(logging_config_path='config/logging_config.yaml', default_level=logging.INFO):
    """Setup logging configuration"""
    try:
        config_path = Path(logging_config_path)
        if config_path.exists():
            with open(config_path, 'rt') as f:
                config = yaml.safe_load(f.read())
            # Create logs directory if it doesn't exist
            logs_dir = Path('logs')
            logs_dir.mkdir(exist_ok=True)
            logging.config.dictConfig(config)
        else:
            logging.basicConfig(level=default_level)
            logging.warning(f"Logging config file not found at {config_path}. Using default config.")
    except Exception as e:
        logging.basicConfig(level=default_level)
        logging.warning(f"Error loading logging config. Using default config. Error: {str(e)}")

def get_logger(name):
    """Get a logger instance with the given name"""
    return logging.getLogger(name)