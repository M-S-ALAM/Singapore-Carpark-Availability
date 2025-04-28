"""
constants/constant.py
========================
Author: Shahbaz
Date: 2023-10-01
Description:
This module contains constants and configuration settings for the project.
It includes file paths, Kafka settings, and other constants used throughout the application.
It is designed to be imported and used in other modules, providing a centralized location for configuration.
The constants are organized into classes for better structure and readability.
The FilePaths class contains paths for data, logs, and configuration files.
The Config class contains settings for data URLs, Kafka broker, topics, and logging levels.
It is important to keep this module updated with any changes to file paths or configuration settings
"""
import os
from dotenv import load_dotenv
# File paths
class FilePaths:
    """
    File paths for the project.
    """
    def __init__(self):
        self.data_dir = "/home/shahbaz/Project/Ongoing_project/DataEngineering/Database"
        self.log_dir = "/home/shahbaz/Project/Ongoing_project/DataEngineering/logs"
        self.config_file = "/home/shahbaz/Project/Ongoing_project/DataEngineering/config/config.yaml"




class Config:
    """
    Configuration class to hold constants and settings for the project.
    """
    def __init__(self):
        self.data_url = os.getenv("DATA_URL", "https://api.data.gov.sg/v1/transport/")
        self.kafka_broker = "localhost:9092"
        self.kafka_topic = "carpark_availability"
        self.kafka_group_id = "carpark_consumer_group"
        self.log_level = "DEBUG"
        self.auto_offset_reset = "latest"
        self.consumer_timeout = 1.0
        self.consumer_poll_interval = 1.0