# FileWatcher

## Environmental Files

Create two `.env` files with the configuration for local runs and for container deployment.

### .env (local testing)
```yaml
# Directory where CSV files are located
DATA_DIRECTORY=C:\\path\\to\\local\\data

# Kafka configuration
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=csv_deltas
CHUNK_SIZE_ROWS=1000
MAX_CHUNK_BYTES=20971520

STATE_FILE=/app/state/csv_kafka_state.pkl

# New SEF schema events topic
SEF_SCHEMA_TOPIC=filewatcher.events
SEF_SOURCE_SYSTEM=csv_filewatcher
SEF_DOMAIN=demo
```

### .env.docker (docker deployment)
```yaml
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

# New SEF schema events topic
SEF_SCHEMA_TOPIC=filewatcher.events
SEF_SOURCE_SYSTEM=csv_filewatcher
SEF_DOMAIN=demo
```

## Deployment and Scaling

### Docker Compose
To start the service locally with Docker Compose run:
```bash
docker-compose up -d
```

### Kubernetes
The `k8s` directory contains example manifests for running FileWatcher in a Kubernetes cluster.
Deploy them with:
```bash
kubectl apply -f k8s/persistent-volume.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/hpa.yaml
```

Scale the deployment manually if needed:
```bash
kubectl scale deployment filewatcher --replicas=<desired_number>
```
The included Horizontal Pod Autoscaler (`k8s/hpa.yaml`) automatically adjusts
replicas based on CPU usage.

### Spawning a New Cluster
If a new cluster is required, create one using tools such as `kind`, `kubeadm`,
or a managed Kubernetes provider. Once the cluster is available, apply the
manifests from the `k8s` folder to deploy FileWatcher.