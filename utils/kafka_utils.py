import traceback

from kafka import KafkaProducer
from logger import log

from config.config import KAFKA_BROKER


def get_kafka_producer():
    try:
        log.info(f"Creating Kafka producer for broker {KAFKA_BROKER}")
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        log.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        log.critical(f"Failed to create Kafka producer: {e}\n{traceback.format_exc()}")
        raise


def kafka_send_callback(success_message, error_message):
    def on_success(record_metadata):
        log.info(
            f"{success_message} | Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

    def on_error(excp):
        log.critical(f"{error_message} | Exception: {excp}\n{traceback.format_exc()}")

    return on_success, on_error


def send_df_in_chunks(df, producer, topic, chunk_size_rows=1000, max_bytes=1000000):
    """
    Sends a DataFrame to Kafka in chunks.
    - chunk_size_rows: max rows per chunk (default: 1000)
    - max_bytes: safety limit per message in bytes (default: 1MB)
    """
    total_rows = len(df)
    for start in range(0, total_rows, chunk_size_rows):
        chunk = df.iloc[start:start + chunk_size_rows]
        payload = chunk.to_csv(index=False)
        payload_bytes = payload.encode('utf-8')
        if len(payload_bytes) > max_bytes:
            # If even a chunk is too big (e.g. lots of columns), halve and send recursively
            if chunk_size_rows > 1:
                half = chunk_size_rows // 2
                send_df_in_chunks(chunk.iloc[:half], producer, topic, half, max_bytes)
                send_df_in_chunks(chunk.iloc[half:], producer, topic, half, max_bytes)
            else:
                log.error(f"Single row exceeds max Kafka message size: {len(payload_bytes)} bytes")
            continue

        on_success, on_error = kafka_send_callback(
            f"Sent chunk with {len(chunk)} rows.",
            f"Failed to send chunk with {len(chunk)} rows."
        )
        fut = producer.send(topic, payload_bytes)
        fut.add_callback(on_success)
        fut.add_errback(on_error)
