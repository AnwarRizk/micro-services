# Distributed Microservices System

A small distributed system built as a learning project. Every component
runs in Docker.

Service A accepts requests over gRPC and saves an event in a PostgreSQL
outbox table. A relay publishes pending events to Kafka. Service B consumes
them and exposes a running total over REST. Prometheus and Grafana track
latency, CPU, and memory across the pipeline.

## Architecture

<img width="1954" height="1032" alt="Image" src="https://github.com/user-attachments/assets/c2dd32a4-af13-410d-bb45-231eff058d29" />

```
Actor/User --gRPC--> Service A (Node.js) --INSERT--> PostgreSQL (outbox)
                                                          |
                                                          v
                                                     Outbox relay (Node.js)
                                                          |
                                                          v
                                                Kafka topic "number-added"
                                                          |
                                                          v
Actor/User <--REST-- Service B (Python/FastAPI) <-- consumes, running total
```

Every box is its own container, all on Compose's default network, reaching
each other by service name (`postgres`, `kafka`, `service-a`, etc).

Metrics flow: `service-a`, `outbox-relay`, `service-b` each expose
`/metrics` → scraped by Prometheus → visualized in Grafana.

### Components

- **Service A** (`service-a/`, Node.js): exposes a gRPC `Add(a, b)` method.
  It writes a `NumberAdded` event to the `outbox` table and returns the
  event ID. Invalid input returns a gRPC `INVALID_ARGUMENT` error.
- **Outbox relay** (`outbox-relay/`, Node.js): polls unpublished rows,
  publishes them to the Kafka topic `number-added`, then sets `published_at`.
  If a publish fails, the row stays unpublished and is retried later.
- **Service B** (`service-b/`, Python/FastAPI): runs a background
  [aiokafka](https://aiokafka.readthedocs.io/) consumer. It adds each event
  value to a running total and saves the total to `sum_state.json`.
  `GET /sum` returns the current total.
- **PostgreSQL**: stores the outbox table.
- **Kafka**: the official [`apache/kafka`](https://hub.docker.com/r/apache/kafka)
  image in KRaft mode (no Zookeeper).
- **Kafka UI**: [`provectuslabs/kafka-ui`](https://github.com/provectus/kafka-ui)
  for browsing topics and messages.
- **Prometheus**: scrapes `/metrics` from all three services every 5 seconds.
- **Grafana**: dashboards for latency, CPU, and memory.

## Project structure

```
.
├── docker-compose.yml        # All 8 services
├── db/init.sql               # Creates the outbox table on first Postgres start
├── proto/adder.proto         # gRPC contract for Service A
├── prometheus/prometheus.yml
├── grafana/dashboards/adder-pipeline.json   # Import this into Grafana
├── service-a/                # Node.js gRPC server + outbox writer
├── outbox-relay/             # Node.js Postgres-to-Kafka relay
├── service-b/                # Python FastAPI + Kafka consumer
└── load-test/grpc-test.js    # K6 load test
```

Note: `service-a/Dockerfile` builds from the **project root** as context
(it needs `proto/`). The other two Dockerfiles use their own local folder.

## Getting started

```bash
docker compose up -d --build
```

Starts all 8 containers: PostgreSQL, Kafka, Kafka UI, Prometheus, Grafana,
Service A, the outbox relay, and Service B. `--build` is needed on first
run and any time you change code inside `service-a/`, `outbox-relay/`, or
`service-b/` — otherwise Compose reuses the old image.

```bash
docker compose ps    # confirm all 8 are running/healthy
```

**Try it:**

```bash
docker compose exec service-a node client_test.js 5 7
curl http://localhost:8000/sum
```

**Logs:** `docker compose logs -f <service-name>`

## Ports

| Port    | What                     |
| ------- | ------------------------ |
| `50051` | Service A (gRPC)         |
| `8000`  | Service B (REST)         |
| `8080`  | Kafka UI                 |
| `9090`  | Prometheus               |
| `3000`  | Grafana                  |
| `5433`  | PostgreSQL (host access) |
| `9094`  | Kafka (host access)      |

## Observability

1. Check `http://localhost:9090/targets` — four targets, all `UP`.
2. In Grafana (`localhost:3000`, `admin`/`admin`): add a Prometheus data
   source at `http://prometheus:9090`, then **Dashboards → Import** and
   upload `grafana/dashboards/adder-pipeline.json`.

Key metric: `consume_latency_seconds` (Service B) — total latency from
Service A's write to Service B's consumption, since `produced_at` is set
in Service A. It's a histogram (cumulative), so use `rate()` in queries:

```
rate(consume_latency_seconds_sum[5m]) / rate(consume_latency_seconds_count[5m])
histogram_quantile(0.95, rate(consume_latency_seconds_bucket[5m]))
```

## Load testing

```bash
cd load-test
k6 run grpc-test.js
```

Ramps gRPC traffic to Service A up to 150 req/s. Before running, clean up
so old data doesn't skew results:

```bash
docker exec -it micro-services-postgres-1 psql -U anwar -d sumdb -c "TRUNCATE TABLE outbox;"
# delete the "number-added" topic in Kafka UI — it's recreated on next publish
docker compose stop service-a outbox-relay service-b
docker compose exec service-b rm -f /data/sum_state.json
docker compose up -d service-a outbox-relay service-b
```

**Finding:** the relay's throughput is capped at `BATCH_SIZE /
POLL_INTERVAL_MS` (default: 100 rows / 1s ≈ 100 events/sec). Push past that
rate and latency climbs — this is expected queueing, not a bug. Both
values are configurable via environment variable.

## Environment variables

Already set correctly in `docker-compose.yml`. Defaults below apply if
running a service outside Docker.

| Variable            | Default (Docker)       | Used by          |
| ------------------- | ---------------------- | ---------------- |
| `POSTGRES_HOST`     | `postgres`             | Service A, Relay |
| `POSTGRES_PORT`     | `5432`                 | Service A, Relay |
| `POSTGRES_DB`       | `sumdb`                | Service A, Relay |
| `POSTGRES_USER`     | `anwar`                | Service A, Relay |
| `POSTGRES_PASSWORD` | `1234`                 | Service A, Relay |
| `KAFKA_BROKER`      | `kafka:9092`           | Relay, Service B |
| `KAFKA_TOPIC`       | `number-added`         | Relay, Service B |
| `KAFKA_GROUP_ID`    | `service-b-group`      | Service B        |
| `POLL_INTERVAL_MS`  | `1000`                 | Relay            |
| `BATCH_SIZE`        | `100`                  | Relay            |
| `SUM_FILE_PATH`     | `/data/sum_state.json` | Service B        |
| `GRPC_PORT`         | `50051`                | Service A        |

Default credentials are for local dev only — never reuse them anywhere real.

## Known gotchas

- **`db/init.sql` runs once**, only on an empty Postgres volume. `docker
compose down -v` wipes it (and Grafana/Service B's saved state) — re-run
  and re-import the dashboard JSON afterward.
- **The relay is required.** Service A doesn't publish to Kafka directly —
  without the relay running, events sit in the outbox with `published_at
IS NULL` forever.
- **Kafka needs `condition: service_healthy`** everywhere it's depended on,
  not `service_started` — the broker process can be running before its
  consumer-group coordinator is actually ready, which caused an
  intermittent "Service B misses the first event" bug.
- **"network not found" from Compose** is usually stale Docker state: `docker
compose down --remove-orphans`, then restart Docker if it persists.
