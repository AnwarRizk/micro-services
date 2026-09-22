# Microservices Project

A small distributed system built as a learning project. Service A accepts
requests over gRPC, stores an event in a PostgreSQL outbox, and a separate
relay publishes pending events to Kafka. Service B consumes those events and
exposes the running total over REST.

## Current architecture

```
Actor/User --gRPC--> Service A (Node.js) --writes--> PostgreSQL outbox
                       |
                       v
                     Outbox relay
                       |
                       v
                   Kafka topic "number-added"
                       |
                       v
Actor/User <--REST-------------------------------- Service B (Python/FastAPI)
                     (consumes, accumulates,
                      persists to a file)
```

- **Service A** (`service-a/`, Node.js): exposes a gRPC `Add(a, b)` method.
  After computing the sum, it writes a `NumberAdded` event to the PostgreSQL
  `outbox` table and returns the generated event ID. Invalid inputs return a
  gRPC `INVALID_ARGUMENT` error.
- **Outbox relay** (`outbox-relay/`, Node.js): polls unpublished rows from
  PostgreSQL, publishes them to the Kafka topic `number-added`, and marks each
  successfully published row with `published_at`.
- **Service B** (`service-b/`, Python/FastAPI): runs a background Kafka
  consumer (via [aiokafka](https://aiokafka.readthedocs.io/)) that reads
  every event from `number-added`, adds its `value` to a running total, and
  writes that total to `sum_state.json`. `GET /sum` returns the current total.
- **PostgreSQL**: stores the transactional outbox used by Service A and the
  relay. The local database is exposed on port `5433`.
- **Kafka**: the official [`apache/kafka`](https://hub.docker.com/r/apache/kafka)
  image, running in KRaft mode (no separate Zookeeper container needed).
- **Kafka UI**: [`provectuslabs/kafka-ui`](https://github.com/provectus/kafka-ui)
  for browsing topics and messages in a browser.

## Project structure

```
.
├── docker-compose.yml       # PostgreSQL, Kafka, and Kafka UI
├── proto/
│   └── adder.proto          # gRPC contract for Service A
├── service-a/                # Node.js gRPC server + outbox writer
│   ├── server.js
│   ├── client_test.js        # manual CLI test client
│   └── package.json
├── outbox-relay/             # Node.js PostgreSQL-to-Kafka relay
│   ├── relay.js
│   └── package.json
└── service-b/                 # Python FastAPI + Kafka consumer
    ├── main.py
    ├── requirements.txt
    ├── generated/             # protoc-generated stubs (not currently used by main.py)
    └── sum_state.json         # local persisted aggregate state
```

## Prerequisites

- Node.js (v18+) and npm
- Python 3.12+ and pip
- Docker + Docker Compose

## Getting started

### 1. Start infrastructure

```bash
docker compose up postgres kafka kafka-ui
```

Kafka UI will be available at http://localhost:8080. Kafka itself listens
on `localhost:9094` for processes running directly on your host. PostgreSQL
is available at `localhost:5433`.

Create the outbox table once after PostgreSQL starts:

```bash
docker compose exec postgres psql -U anwar -d sumdb -c \
  "CREATE TABLE IF NOT EXISTS outbox (
     id BIGSERIAL PRIMARY KEY,
     event_type TEXT NOT NULL,
     payload JSONB NOT NULL,
     created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
     published_at TIMESTAMPTZ
   );"
```

### 2. Start Service A

```bash
cd service-a
npm install
npm start
```

You should see `Service A (gRPC) listening on 0.0.0.0:50051`.

### 3. Start the outbox relay

In another terminal:

```bash
cd outbox-relay
npm install
npm start
```

The relay polls every two seconds by default. It publishes each pending row
to Kafka and then sets its `published_at` timestamp. A failed row remains
unpublished and is retried during a later poll.

### 4. Start Service B

```bash
cd service-b
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Regenerate the Python gRPC stubs after proto/adder.proto changes if Service B
# needs the gRPC contract. main.py does not import them today.
python -m grpc_tools.protoc -I../proto --python_out=generated --grpc_python_out=generated ../proto/adder.proto

uvicorn main:app --reload --port 8000
```

### 5. Try it

Call Service A's `Add` RPC with the included client:

```bash
cd service-a
node client_test.js 5 7
```

The response includes the computed sum and the outbox event ID. After the
relay publishes the event and Service B consumes it, check the running total:

```bash
curl http://localhost:8000/sum
```

Example response:

```json
{
  "sum": 12.0,
  "updated_at": "2026-09-21T22:32:46.377088+00:00",
  "last_event_id": "..."
}
```

`GET /healthz` returns `{"status":"ok"}` for a basic health check. The
Kafka topic is created automatically on the first publish in this single-node
development setup.

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

- **Create the outbox table first.** `docker-compose.yml` starts PostgreSQL
  but does not run migrations. Service A will return `UNAVAILABLE` until the
  table from the setup step exists.
- **The relay is required.** Service A no longer publishes directly to Kafka.
  Start the relay after PostgreSQL and Kafka are available, or events will
  remain in the outbox with `published_at` set to `NULL`.
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

| Service   | Variable            | Default           | Purpose                              |
| --------- | ------------------- | ----------------- | ------------------------------------ |
| Service A | `GRPC_PORT`         | `50051`           | gRPC server port                     |
| Service A | `POSTGRES_HOST`     | `localhost`       | PostgreSQL host                      |
| Service A | `POSTGRES_PORT`     | `5433`            | PostgreSQL port                      |
| Service A | `POSTGRES_DB`       | `sumdb`           | PostgreSQL database                  |
| Service A | `POSTGRES_USER`     | `anwar`           | PostgreSQL user                      |
| Service A | `POSTGRES_PASSWORD` | `1234`            | PostgreSQL password                  |
| Relay     | `KAFKA_BROKER`      | `localhost:9094`  | Kafka bootstrap address              |
| Relay     | `KAFKA_TOPIC`       | `number-added`    | Topic to publish to                  |
| Relay     | `POLL_INTERVAL_MS`  | `2000`            | Outbox polling interval              |
| Relay     | `POSTGRES_HOST`     | `localhost`       | PostgreSQL host                      |
| Relay     | `POSTGRES_PORT`     | `5433`            | PostgreSQL port                      |
| Relay     | `POSTGRES_DB`       | `sumdb`           | PostgreSQL database                  |
| Relay     | `POSTGRES_USER`     | `anwar`           | PostgreSQL user                      |
| Relay     | `POSTGRES_PASSWORD` | `1234`            | PostgreSQL password                  |
| Service B | `KAFKA_BROKER`      | `localhost:9094`  | Kafka bootstrap address              |
| Service B | `KAFKA_TOPIC`       | `number-added`    | Topic to consume from                |
| Service B | `KAFKA_GROUP_ID`    | `service-b-group` | Consumer group ID                    |
| Service B | `SUM_FILE_PATH`     | `sum_state.json`  | Where the running total is persisted |
