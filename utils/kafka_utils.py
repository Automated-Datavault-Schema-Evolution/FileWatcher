import hashlib
import traceback
import json
import os
from typing import Any, Dict, List
import re
from uuid import uuid4

from kafka import KafkaProducer
from logger import log
from datetime import datetime
from zoneinfo import ZoneInfo

from config.config import KAFKA_BROKER, SEF_SCHEMA_TOPIC, SEF_DOMAIN, SEF_SOURCE_SYSTEM

_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")

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
    """Build a SchemaNotification for a given CSV file.

    Key guarantees:
      - Read sample as strings (no NaN coercion).
      - Infer logical types from non-empty values only.
      - If a column has no non-empty sample values, default to 'string'.
    """
    import pandas as pd

    # 1) Always read header first (fast, robust)
    try:
        header_df = pd.read_csv(
            file_path,
            nrows=0,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
        )
    except Exception as e:
        log.error(f"Failed to read header from {file_path}: {e}")
        return None

    # 2) Read a sample for value-based inference (best effort)
    try:
        sample_df = pd.read_csv(
            file_path,
            nrows=1000,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
        )
    except Exception as e:
        # If file is being written concurrently, this can happen.
        # Proceed with header only; default types to string.
        log.warning(f"Failed to read sample from {file_path}, using header-only typing: {e}")
        sample_df = None

    attributes: List[Dict[str, Any]] = []
    for col in header_df.columns:
        raw_vals: List[str] = []

        if sample_df is not None and col in sample_df.columns and len(sample_df) > 0:
            # With dtype=str + na_filter=False, values should already be strings; normalize anyway.
            raw_vals = ["" if v is None else str(v) for v in sample_df[col].tolist()]

        non_empty = [v.strip() for v in raw_vals if v is not None and str(v).strip() != ""]
        logical_type = _infer_logical_type_from_non_empty_values(non_empty)

        # If we have no sample rows, treat as nullable (safe default).
        nullable = True if not raw_vals else any(v.strip() == "" for v in raw_vals)

        attributes.append(
            {
                "name": col,
                "logical_type": logical_type,
                "physical_type": "string",
                "nullable": nullable,
            }
        )

    schema_fingerprint = hashlib.md5(
        json.dumps(attributes, sort_keys=True).encode("utf-8")
    ).hexdigest()

    dataset_id = os.path.splitext(os.path.basename(file_path))[0]
    observed_at = datetime.now(ZoneInfo("Europe/Vienna")).isoformat()

    try:
        size_bytes = os.stat(file_path).st_size
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

def send_schema_notification(
    producer: KafkaProducer,
    notification: Dict[str, Any],
    topic: str | None = None,
) -> None:
    """Send a pre-built SchemaNotification dict to Kafka (no rebuilding/re-reading)."""
    if notification is None:
        return

    topic_name = topic or SEF_SCHEMA_TOPIC
    file_ref = (
        (notification.get("ingestion") or {}).get("file_path")
        or (notification.get("dataset") or {}).get("id")
        or "unknown"
    )

    payload_bytes = json.dumps(notification).encode("utf-8")

    on_success, on_error = kafka_send_callback(
        f"Sent schema notification for {file_ref}.",
        f"Failed to send schema notification for {file_ref}.",
    )
    log.debug(f"Sending schema.notification for {file_ref} to topic {topic_name}")
    fut = producer.send(topic_name, payload_bytes)
    fut.add_callback(on_success)
    fut.add_errback(on_error)

    try:
        producer.flush()
    except Exception as e:
        log.error(f"Failed to flush Kafka producer after schema notification for {file_ref}: {e}")

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

    send_schema_notification(producer, notification, topic=topic)


def _infer_logical_type_from_non_empty_values(values: List[str]) -> str:
    """
    Infer logical type by inspecting *non-empty* values.

    This prevents pandas from defaulting to float when a column is entirely empty
    (NaN) in the sample.

    Rules (in order):
      - empty -> string
      - boolean
      - integer
      - float
      - timestamp (ISO-ish)
      - string
    """
    if not values:
        return "string"

    vals = [str(v).strip() for v in values if v is not None and str(v).strip() != ""]
    if not vals:
        return "string"

    lowered = {v.lower() for v in vals}
    if lowered.issubset({"true", "false", "0", "1", "yes", "no", "y", "n"}):
        return "boolean"

    if all(_INT_RE.match(v) for v in vals):
        return "integer"

    if all(_FLOAT_RE.match(v) for v in vals):
        return "float"

    # Timestamp heuristic: only attempt parsing if it looks date/time-ish,
    # to avoid mis-parsing plain integers.
    if any(any(ch in v for ch in ("-", "/", ":", "T")) for v in vals):
        try:
            import pandas as _pd
            parsed = _pd.to_datetime(vals, errors="coerce", utc=False)
            ok = int(parsed.notna().sum())
            if ok / max(1, len(vals)) >= 0.9:
                return "timestamp"
        except Exception:
            pass

    return "string"