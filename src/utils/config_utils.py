import json
from pathlib import Path

def load_config(config_path='config/app_config.json'):
    """Load application configuration from JSON file"""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        raise Exception(f"Config file not found at {config_path}")
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON in config file at {config_path}")

def get_spark_config(config):
    """Extract Spark configuration from app config"""
    return config.get('spark_config', {})