from kafka import KafkaProducer

def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: v.encode("utf-8"),
    )
    print("Enter messages to send. Press Ctrl+D (or Ctrl+Z on Windows) to exit.")
    try:
        while True:
            line = input("message> ")
            if line:
                producer.send("str-topic", line)
                producer.flush()
    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        producer.close()

if __name__ == "__main__":
    main()
