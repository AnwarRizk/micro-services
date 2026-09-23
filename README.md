# Microservices Project

A small distributed system built as a learning project. Service A accepts
requests over gRPC, stores an event in a PostgreSQL outbox, and a separate
relay publishes pending events to Kafka. Service B consumes those events and
exposes the running total over REST.

## Recent updates

- Migrated the flow to a durable PostgreSQL outbox pattern: Service A now
  writes `NumberAdded` events to `outbox` and returns the generated event ID.
- Added a dedicated Node.js relay that polls unpublished outbox rows, sends
  them to Kafka, and marks them as published.
- Switched the Kafka setup to a single-node KRaft configuration with a host
  listener on `localhost:9094`, plus Kafka UI for inspection.
- Service B now runs as a background `aiokafka` consumer, keeps a running
  total in memory, and persists it to `sum_state.json` after each event.
- Local development currently runs Postgres, Kafka, and Kafka UI through
  Docker Compose while Service A and Service B run directly on the host.

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
├── docker-compose.yml       # Runs PostgreSQL, Kafka, and Kafka UI
├── db/
│   └── init.sql             # Creates the outbox table on first Postgres startup
├── proto/
│   └── adder.proto          # gRPC contract for Service A
├── service-a/                # Node.js gRPC server + outbox writer
│   ├── server.js
│   ├── client_test.js        # manual CLI test client
│   └── package.json
├── outbox-relay/             # Node.js PostgreSQL-to-Kafka relay
│   ├── relay.js
│   └── package.json
├── service-b/                 # Python FastAPI + Kafka consumer
│   ├── main.py
│   ├── requirements.txt
│   ├── generated/            # protoc-generated stubs
│   └── sum_state.json        # local persisted aggregate state
├── README.md
└── .gitignore
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

The database is initialized from [db/init.sql](db/init.sql), which creates the
`outbox` table on the first Postgres startup. The table uses a UUID primary key
and stores the event payload as JSONB.

### 2. Start Service A

```bash
cd service-a
npm install
npm start
```

The database is expected to be ready before Service A starts, because the
Compose bootstrap script creates the `outbox` table in PostgreSQL. Once the
service is running you should see `Service A (gRPC) listening on 0.0.0.0:50051`.

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

- **PostgreSQL must be available before Service A starts.** The Compose
  bootstrap script in [db/init.sql](db/init.sql) creates the outbox table on
  first startup. If the database is not ready, Service A will fail when it
  tries to insert its first event.
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
