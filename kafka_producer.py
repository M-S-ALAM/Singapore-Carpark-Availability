import requests
import time
import json
import logging
from quixstreams import Application


def get_weather():
    try:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": 51.5,
                "longitude": -0.11,
                "current": "temperature_2m",
            },
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Weather API error: {e}")
        return None


def main():
    app = Application(
        broker_address="http://192.168.136.32:9092",  # 🔧 Corrected port
        loglevel="DEBUG",
    )

    with app.get_producer() as producer:
        while True:
            weather = get_weather()
            if weather:
                logging.debug("Got weather: %s", weather)
                producer.produce(
                    topic="weather_data_demo",
                    key="London",
                    value=json.dumps(weather),
                )
                logging.info("Produced. Sleeping...")
            else:
                logging.warning("No weather data fetched. Skipping produce.")
            time.sleep(300)  # Sleep for 5 minutes


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
