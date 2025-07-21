import traceback
import json
import os

from kafka import KafkaProducer
from logger import log
from datetime import datetime
from zoneinfo import ZoneInfo

from config.config import KAFKA_BROKER


def get_kafka_producer():
    try:
        log.info(f"Creating Kafka producer for broker {KAFKA_BROKER}")
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, compression_type='gzip', max_request_size=20971520,
                                 linger_ms=20, acks=1)
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


def send_df_in_chunks(df, producer, topic, source_file, chunk_size_rows=1000, max_bytes=1000000):
    """
    Sends a DataFrame to Kafka in chunks.
    - chunk_size_rows: max rows per chunk (default: 1000)
    - max_bytes: safety limit per message in bytes (default: 1MB)
    """
    total_rows = len(df)
    log.debug(f"Sending DataFrame from {source_file} with {total_rows} rows in chunks")
    for start in range(0, total_rows, chunk_size_rows):
        chunk = df.iloc[start:start + chunk_size_rows]
        log.debug(f"Preparing chunk rows {start} to {start + len(chunk) - 1}")
        #payload = chunk.to_csv(index=False)
        msg_dict = {
            "filename": os.path.splitext(os.path.basename(source_file))[0],
            "data_format": "json",
            "ingestion_timestamp": datetime.now(ZoneInfo("Europe/Vienna")).isoformat(),
            "data": json.loads(chunk.to_json(orient="records"))
        }
        payload_bytes = json.dumps(msg_dict).encode("utf-8")
        if len(payload_bytes) > max_bytes:
            # If even a chunk is too big (e.g. lots of columns), halve and send recursively
            log.debug(
                f"Chunk from {source_file} exceeds max size {max_bytes} bytes, current {len(payload_bytes)} bytes"
            )
            if chunk_size_rows > 1:
                half = chunk_size_rows // 2
                log.debug(f"Splitting chunk into halves of {half} rows")
                send_df_in_chunks(chunk.iloc[:half], producer, topic, source_file, half, max_bytes)
                send_df_in_chunks(chunk.iloc[half:], producer, topic, source_file, half, max_bytes)
            else:
                log.error(f"Single row exceeds max Kafka message size: {len(payload_bytes)} bytes")
            continue

        on_success, on_error = kafka_send_callback(
            f"Sent chunk with {len(chunk)} rows from {source_file}.",
            f"Failed to send chunk with {len(chunk)} rows from {source_file}."
        )
        log.debug(f"Sending chunk with {len(chunk)} rows to topic {topic}")
        fut = producer.send(topic, payload_bytes)
        fut.add_callback(on_success)
        fut.add_errback(on_error)
