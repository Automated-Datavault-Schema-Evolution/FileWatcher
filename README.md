# FileWatcher

## Environmental Files

Create 2 .env-files:

.env (for local testing)
````yaml
# Directory where CSV files are located (inside the application container)
DATA_DIRECTORY=C:\Users\alexm\Desktop\repos\automated_datavault_schema_evolution\data


# Kafka configuration (these point to the externally managed Kafka)
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=csv_deltas
CHUNK_SIZE_ROWS=1000
MAX_CHUNK_BYTES=20971520

STATE_FILE=/app/state/csv_kafka_state.pkl

````

.env.docker (for docker deployment)
````yaml
# Directory where CSV files are located (inside the application container)
DATA_DIRECTORY=./data
HOST_DATA_DIRECTORY=C:\Users\alexm\Desktop\repos\automated_datavault_schema_evolution\data


# Kafka configuration (these point to the externally managed Kafka)
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=csv_deltas
CHUNK_SIZE_ROWS=1000
MAX_CHUNK_BYTES=20971520

STATE_FILE=/app/state/csv_kafka_state.pkl
# STATE_FILE=csv_kafka_state.pkl
````