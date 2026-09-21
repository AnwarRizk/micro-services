# Adder Microservices Project

A small distributed system built as a learning project: two services that
talk to each other asynchronously through Kafka, using gRPC and REST as
their respective entry points.

## Current architecture

```
Actor/User --gRPC--> Service A (Node.js) --publishes--> Kafka topic "number-added"
                                                                |
                                                                v
Actor/User --REST----------------------------------> Service B (Python/FastAPI)
                                                        (consumes, accumulates,
                                                         persists to a file)
```

- **Service A** (`service-a/`, Node.js): exposes a gRPC `Add(a, b)` method.
  After computing the sum, it publishes an event to the Kafka topic
  `number-added` via [kafkajs](https://kafka.js.org/).
- **Service B** (`service-b/`, Python/FastAPI): runs a background Kafka
  consumer (via [aiokafka](https://aiokafka.readthedocs.io/)) that reads
  every event from `number-added`, adds it to a running total, and writes
  that total to `sum_state.json`. `GET /sum` returns the current total.
- **Kafka**: the official [`apache/kafka`](https://hub.docker.com/r/apache/kafka)
  image, running in KRaft mode (no separate Zookeeper container needed).
- **Kafka UI**: [`provectuslabs/kafka-ui`](https://github.com/provectus/kafka-ui)
  for browsing topics and messages in a browser.

### What's _not_ built yet

The planned design also includes a Postgres-backed outbox pattern (Service A
writing to a DB table instead of publishing directly, with a separate relay
process), plus OpenTelemetry, Prometheus, and Grafana for observability, and
K6 for load testing. None of that is implemented yet — the current system is
intentionally the simpler "publish directly to Kafka" version. The current
`proto/adder.proto` contains only Service A's gRPC contract; the planned event
schema and Service B REST contract are not checked in yet.

## Project structure

```
.
├── docker-compose.yml       # Kafka + Kafka UI only, for now
├── proto/
│   └── adder.proto          # gRPC contract for Service A
├── service-a/                # Node.js gRPC server + Kafka producer
│   ├── server.js
│   ├── client_test.js        # manual CLI test client
│   └── package.json
└── service-b/                 # Python FastAPI + Kafka consumer
    ├── main.py
    ├── requirements.txt
    └── generated/             # protoc-generated stubs (not currently used by main.py)
```

## Prerequisites

- Node.js (v18+) and npm
- Python 3.12+ and pip
- Docker + Docker Compose

## Getting started

### 1. Start Kafka and Kafka UI

```bash
docker compose up kafka kafka-ui
```

Kafka UI will be available at http://localhost:8080. Kafka itself listens
on `localhost:9094` for processes running directly on your host (Service A
and B are not containerized yet).

### 2. Start Service A

```bash
cd service-a
npm install
npm start
```

You should see `Service A (gRPC) listening on 0.0.0.0:50051` and
`[Kafka] producer connected to localhost:9094`.

### 3. Start Service B

```bash
cd service-b
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Regenerate the Python gRPC stubs if Service B starts using the gRPC contract
# (or after proto/adder.proto changes). They are not imported by main.py today.
python -m grpc_tools.protoc -I../proto --python_out=generated --grpc_python_out=generated ../proto/adder.proto

uvicorn main:app --reload --port 8000
```

### 4. Try it

Call Service A's `Add` RPC (via Postman, or `node client_test.js 5 7` from
`service-a/`), then check the running total:

```bash
curl http://localhost:8000/sum
```

Example response:

```json
{
  "sum": 78.0,
  "updated_at": "2026-09-21T22:32:46.377088+00:00",
  "last_event_id": "..."
}
```

`GET /healthz` returns `{"status":"ok"}` for a basic health check. The
Kafka topic is created automatically by Kafka on the first publish in this
single-node development setup.

### Resetting local state

Service B persists its total in `service-b/sum_state.json` and consumes with a
Kafka consumer group. To replay all retained events from the beginning, stop
Service B, remove the state file, and use a new consumer-group ID:

```bash
cd service-b
rm sum_state.json
KAFKA_GROUP_ID=service-b-replay uvicorn main:app --port 8000
```

Do not remove only the state file while keeping the same consumer group: the
consumer may resume after the stored total has been reset and produce an
inconsistent result. Conversely, reusing an old state file with a new group
can count retained events twice.

## Known gotchas

- **Single-node Kafka replication factor.** Kafka's internal topics default
  to needing 3 replicas. With only one broker, this must be overridden —
  already handled in `docker-compose.yml` via
  `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1` and friends.
- **Listener addresses.** Kafka is configured with separate `PLAINTEXT`
  (for other containers) and `EXTERNAL` (for host processes, port `9094`)
  listeners. If you containerize Service A/B later, they'll need to switch
  to `kafka:9092` instead of `localhost:9094`.
- **"network not found" from Docker Compose.** Usually a stale Docker
  Desktop state. Fix: `docker compose down --remove-orphans`, then restart
  Docker itself if the error persists.

## Environment variables

| Service   | Variable         | Default           | Purpose                              |
| --------- | ---------------- | ----------------- | ------------------------------------ |
| Service A | `KAFKA_BROKER`   | `localhost:9094`  | Kafka bootstrap address              |
| Service A | `KAFKA_TOPIC`    | `number-added`    | Topic to publish to                  |
| Service A | `GRPC_PORT`      | `50051`           | gRPC server port                     |
| Service B | `KAFKA_BROKER`   | `localhost:9094`  | Kafka bootstrap address              |
| Service B | `KAFKA_TOPIC`    | `number-added`    | Topic to consume from                |
| Service B | `KAFKA_GROUP_ID` | `service-b-group` | Consumer group ID                    |
| Service B | `SUM_FILE_PATH`  | `sum_state.json`  | Where the running total is persisted |
