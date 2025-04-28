#!/usr/bin/env python

import os
import json
import time
import logging
import requests
from typing import Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
from constants.constant import FilePaths, Config

class LTAApiHandler:
    """Handler for LTA DataMall API operations."""

    def __init__(self):
        """
        Initialize LTA API handler.
        """
        self._setup_logger()
        self.base_url = Config().data_url

        # Create data directory
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
        os.makedirs(self.data_dir, exist_ok=True)

    def _setup_logger(self) -> None:
        """Configure the logger."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_carpark_availability(self) -> List[Dict]:
        """
        Get carpark availability data.

        Returns:
            List[Dict]: List containing carpark data.
        """
        endpoint = "carpark-availability"
        url = self.base_url.rstrip('/') + '/' + endpoint  # safe URL construction

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Correct JSON parsing
            carparks = data.get('items', [{}])[0].get('carpark_data', [])

            self._enrich_carpark_data(carparks)

            self.logger.info(f"Successfully retrieved {len(carparks)} carpark records.")
            return carparks

        except requests.RequestException as e:
            error_msg = f"API request failed: {str(e)}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

    def _enrich_carpark_data(self, carparks: List[Dict]) -> None:
        """
        Enrich carpark data with timestamp and parsed coordinates.

        Args:
            carparks (List[Dict]): List of carpark data.
        """
        timestamp = datetime.now().isoformat()

        for carpark in carparks:
            carpark['timestamp'] = timestamp

            location = carpark.get('Location')
            if location:
                try:
                    lat, lng = map(float, location.split())
                    carpark['Latitude'] = lat
                    carpark['Longitude'] = lng
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Failed to parse location for carpark: {e}")
                    carpark['Latitude'] = None
                    carpark['Longitude'] = None

    def save_to_local(self, data: List[Dict], filename: Optional[str] = None) -> str:
        """
        Save data to local file.

        Args:
            data (List[Dict]): Data to save.
            filename (Optional[str]): Filename, auto-generated if not provided.

        Returns:
            str: Path to the saved file.
        """
        if not data:
            self.logger.warning("No data to save.")
            return ""

        if filename is None:
            filename = f"carpark_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        filepath = os.path.join(self.data_dir, filename)

        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            self.logger.info(f"Data saved to: {filepath}")
            return filepath
        except Exception as e:
            error_msg = f"Error saving data: {str(e)}"
            self.logger.error(error_msg)
            raise IOError(error_msg)

    def to_dataframe(self, data: List[Dict]) -> 'pd.DataFrame':
        """
        Convert data to Pandas DataFrame.

        Args:
            data (List[Dict]): List of carpark data.

        Returns:
            pd.DataFrame: DataFrame containing carpark data.
        """
        try:
            import pandas as pd
            df = pd.DataFrame(data)
            self.logger.info(f"Successfully converted to DataFrame, shape: {df.shape}.")
            return df
        except ImportError:
            self.logger.error("Pandas not installed. Please install it using: pip install pandas.")
            raise ImportError("Please install pandas: pip install pandas")

    def send_to_kafka(self, data: List[Dict], topic: str = "carpark-availability") -> int:
        """
        Send data to Kafka topic.

        Args:
            data (List[Dict]): Data to send.
            topic (str): Kafka topic name.

        Returns:
            int: Number of records sent.
        """
        if not data:
            self.logger.warning("No data to send to Kafka.")
            return 0

        try:
            producer = KafkaProducer(
                bootstrap_servers=['34.126.86.205:9093'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )

            for record in data:
                producer.send(topic, value=record)

            producer.flush()
            producer.close()

            self.logger.info(f"Successfully sent {len(data)} records to Kafka topic '{topic}'.")
            return len(data)
        except Exception as e:
            error_msg = f"Failed to send data to Kafka: {str(e)}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

def main():
    """
    Main execution loop.
    """
    api_handler = LTAApiHandler()

    while True:
        try:
            carpark_data = api_handler.get_carpark_availability()

            if carpark_data:
                api_handler.save_to_local(carpark_data)
                api_handler.send_to_kafka(carpark_data)
                print(f"Successfully processed and saved {len(carpark_data)} records.")
            else:
                print("No carpark data received.")

            print("Waiting 5 minutes before next data fetch...")
            time.sleep(300)

        except Exception as e:
            logging.error(f"Error occurred: {str(e)}")
            print("Retrying after 60 seconds...")
            time.sleep(60)

if __name__ == "__main__":
    main()
