from kafka import KafkaConsumer

def main():
    consumer = KafkaConsumer(
        "str-topic",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: v.decode("utf-8"),
        auto_offset_reset="earliest",
        group_id="str-consumer-group",
    )
    print("Listening for messages. Press Ctrl+C to exit.")
    try:
        for message in consumer:
            print(f"Received: {message.value}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
