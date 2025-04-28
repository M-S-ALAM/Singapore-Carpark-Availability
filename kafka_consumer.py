from quixstreams import Application
import json


def main():
    app = Application(
        broker_address="http://192.168.136.32:9092",  # ✅ Match actual Kafka port
        loglevel="DEBUG",
        consumer_group="weather_reader",
        auto_offset_reset="latest",
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["weather_data_demo"])

        while True:
            msg = consumer.poll(1.0)  # ✅ Poll every 1 sec

            if msg is None:
                print("Waiting...")
            elif msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            else:
                key = msg.key().decode("utf-8") if msg.key() else None
                value = json.loads(msg.value())
                offset = msg.offset()

                print(f"{offset} {key} {value}")
                consumer.store_offsets(msg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nConsumer shutdown.")
