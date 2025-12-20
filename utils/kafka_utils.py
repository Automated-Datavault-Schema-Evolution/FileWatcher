import hashlib
import traceback
import json
import os
from typing import Any, Dict, List
from uuid import uuid4

from kafka import KafkaProducer
from logger import log
from datetime import datetime
from zoneinfo import ZoneInfo

from config.config import KAFKA_BROKER, SEF_SCHEMA_TOPIC, SEF_DOMAIN, SEF_SOURCE_SYSTEM

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

        # Ensure all pending messages are sent, especially for large batches
    try:
        producer.flush()
        log.debug(f"Kafka producer flushed after sending {total_rows} rows from {source_file}")
    except Exception as e:
        log.error(f"Failed to flush Kafka producer: {e}")

def _infer_logical_type_from_dtype(dtype_str: str) -> str:
    lower = dtype_str.lower()
    if "int" in lower:
        return "integer"
    if "float" in lower or "double" in lower:
        return "float"
    if "bool" in lower:
        return "boolean"
    if "datetime" in lower or "date" in lower:
        return "timestamp"
    return "string"


def build_schema_notification(file_path: str) -> Dict[str, Any] | None:
    """Build a SchemaNotification for a given CSV file."""
    import pandas as pd

    try:
        df_sample = pd.read_csv(file_path, nrows=100)
    except Exception as e:
        log.error(f"Failed to read {file_path} for schema notification: {e}")
        return None

    # Ensure we at least have the header
    if df_sample is None:
        try:
            df_sample = pd.read_csv(file_path, nrows=0)
        except Exception as e:
            log.error(f"Failed to read header from {file_path}: {e}")
            return None

    attributes: List[Dict[str, Any]] = []
    for col in df_sample.columns:
        series = df_sample[col]
        dtype_str = str(series.dtype)
        logical_type = _infer_logical_type_from_dtype(dtype_str)
        nullable = bool(series.isna().any()) if len(series) else True
        attributes.append(
            {
                "name": col,
                "logical_type": logical_type,
                "physical_type": dtype_str,
                "nullable": nullable,
            }
        )

    schema_fingerprint = hashlib.md5(
        json.dumps(attributes, sort_keys=True).encode("utf-8")
    ).hexdigest()
    dataset_id = os.path.splitext(os.path.basename(file_path))[0]

    observed_at = datetime.now(ZoneInfo("Europe/Vienna")).isoformat()
    try:
        stat = os.stat(file_path)
        size_bytes = stat.st_size
    except OSError:
        size_bytes = 0

    notification: Dict[str, Any] = {
        "event_type": "schema.notification",
        "dataset": {
            "id": dataset_id,
            "source": SEF_SOURCE_SYSTEM,
            "zone": "raw",
        },
        "header": {
            "attributes": attributes,
            "primary_key": [],
            "schema_fingerprint": schema_fingerprint,
        },
        "ingestion": {
            "observed_at": observed_at,
            "file_path": file_path,
            "size_bytes": size_bytes,
        },
        "previous_schema_version": None,
        "metadata": {
            "domain": SEF_DOMAIN,
        },
    }
    return notification


def send_schema_notification_for_file(
    producer: KafkaProducer,
    file_path: str,
    topic: str | None = None,
) -> None:
    """Build and send a SchemaNotification for the given file to Kafka."""
    notification = build_schema_notification(file_path)
    if notification is None:
        log.debug(f"No schema notification built for {file_path}; skipping send.")
        return

    topic_name = topic or SEF_SCHEMA_TOPIC
    payload_bytes = json.dumps(notification).encode("utf-8")

    on_success, on_error = kafka_send_callback(
        f"Sent schema notification for {file_path}.",
        f"Failed to send schema notification for {file_path}.",
    )
    log.debug(f"Sending schema.notification for {file_path} to topic {topic_name}")
    fut = producer.send(topic_name, payload_bytes)
    fut.add_callback(on_success)
    fut.add_errback(on_error)

    try:
        producer.flush()
    except Exception as e:
        log.error(f"Failed to flush Kafka producer after schema notification for {file_path}: {e}")