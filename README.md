# Microservices Project

A small distributed system built as a learning project.

Service A accepts requests over gRPC and saves an event in a PostgreSQL
outbox table. A separate relay publishes pending events to Kafka. Service B
consumes those events and exposes the running total over REST. Prometheus
and Grafana show how fast events move through the pipeline.

## Architecture

<img width="700" alt="Architecture diagram" src="https://github.com/user-attachments/assets/bc0bd689-f11e-4dc1-ab76-e181e90187a5" />

```
Actor/User --gRPC--> Service A (Node.js) --INSERT--> PostgreSQL (outbox table)
                                                          |
                                                          | polls every 1s (batch size: 100)
                                                          v
                                                     Outbox relay (Node.js)
                                                          |
                                                          v
                                                Kafka topic "number-added"
                                                          |
                                                          v
Actor/User <--REST-- Service B (Python/FastAPI) <-- consumes, keeps running total
```

Observability:

```
Service A     :9100/metrics --+
Outbox relay  :9101/metrics --+--> Prometheus :9090 --> Grafana :3000
Service B     :8000/metrics --+
```

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

## Ports

| Port    | What                            |
| ------- | ------------------------------- |
| `50051` | Service A (gRPC)                |
| `9100`  | Service A metrics               |
| `9101`  | Outbox relay metrics            |
| `8000`  | Service B (REST and `/metrics`) |
| `5433`  | PostgreSQL (host port)          |
| `9094`  | Kafka (host listener)           |
| `8080`  | Kafka UI                        |
| `9090`  | Prometheus                      |
| `3000`  | Grafana                         |

## Project structure

```
.
├── docker-compose.yml        # PostgreSQL, Kafka, Kafka UI, Prometheus, Grafana
├── db/
│   └── init.sql              # Creates the outbox table on first Postgres start
├── proto/
│   └── adder.proto           # gRPC contract for Service A
├── prometheus/
│   └── prometheus.yml        # Scrape targets
├── grafana/
│   └── dashboards/
│       └── adder-pipeline.json   # Exported dashboard (import it into Grafana)
├── service-a/                # Node.js gRPC server + outbox writer
│   ├── server.js
│   ├── client_test.js        # manual CLI test client
│   └── package.json
├── outbox-relay/             # Node.js PostgreSQL-to-Kafka relay
│   ├── relay.js
│   └── package.json
├── service-b/                # Python FastAPI + Kafka consumer
│   ├── main.py
│   └── requirements.txt
├── load-test/
│   └── grpc-test.js          # K6 load test against Service A's gRPC Add
├── README.md
└── .gitignore
```

`service-b/sum_state.json` is created at runtime and is ignored by git.

## Prerequisites

- Node.js and npm. Node 22 or newer is recommended, because
  `@prometheus-io/client` asks for it. Node 20 still works, but npm prints
  an engine warning.
- Python 3.12+ and pip
- Docker and Docker Compose

## Getting started

Service A, the relay, and Service B run directly on your machine. Everything
else runs in Docker.

### 1. Start the infrastructure

```bash
docker compose up -d
```

This starts PostgreSQL, Kafka, Kafka UI, Prometheus, and Grafana.

The `outbox` table is created from [db/init.sql](db/init.sql) the first time
Postgres starts on an empty volume. See the gotchas below if you need to run
it again.

### 2. Start Service A

```bash
cd service-a
npm install
npm start
```

You should see `Service A (gRPC) listening on 0.0.0.0:50051` and
`Metrics server listening on 0.0.0.0:9100`.

### 3. Start the outbox relay

In another terminal:

```bash
cd outbox-relay
npm install
npm start
```

The relay polls every 1 second by default. Its metrics are on port `9101`.

### 4. Start Service B

```bash
cd service-b
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

`--host 0.0.0.0` is required. Without it, Prometheus (inside Docker) cannot
reach Service B and the scrape fails with "connection refused".

### 5. Try it

```bash
cd service-a
node client_test.js 5 7
```

The response contains the sum and the outbox event ID. After the relay
publishes the event and Service B consumes it:

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

`GET /healthz` returns `{"status":"ok"}`.

## Observability

### Check that Prometheus is scraping

Open http://localhost:9090/targets. You should see four targets, all `UP`:
`prometheus`, `service-a`, `outbox-relay`, and `service-b`.

### Set up Grafana

1. Open http://localhost:3000 and log in with `admin` / `admin`.
2. Go to **Connections -> Data sources -> Add data source -> Prometheus**.
3. Set the URL to `http://prometheus:9090` and click **Save & test**.
4. Go to **Dashboards -> New -> Import**, upload
   `grafana/dashboards/adder-pipeline.json`, and choose the Prometheus
   data source.

Grafana data is stored in the `grafana-data` volume. If you delete volumes,
re-import the dashboard from the JSON file.

### Metrics

| Metric                              | Service      | Type      | What it measures                                               |
| ----------------------------------- | ------------ | --------- | -------------------------------------------------------------- |
| `add_requests_total`                | Service A    | Counter   | Successful `Add` calls                                         |
| `outbox_publish_latency_seconds`    | Outbox relay | Histogram | Time from outbox row creation to Kafka publish                 |
| `consume_latency_seconds`           | Service B    | Histogram | Time from `produced_at` (set in Service A) to consumption in B |
| `process_*`, `nodejs_*`, `python_*` | all          | Default   | CPU, memory, event loop, garbage collection                    |

`consume_latency_seconds` is the **total** latency from Service A to
Service B, because `produced_at` is set when Service A writes the event.

Histograms only count up. Use `rate()` in Grafana to see recent behavior:

```
# Average latency over 5 minutes
rate(consume_latency_seconds_sum[5m]) / rate(consume_latency_seconds_count[5m])

# 95th percentile latency
histogram_quantile(0.95, rate(consume_latency_seconds_bucket[5m]))

# CPU per service (1.0 = one full core)
rate(process_cpu_seconds_total[5m])

# Memory per service
process_resident_memory_bytes
```

If a service was stopped for a while, the events waiting in the outbox or
Kafka show up as very large latencies when it starts again. That is real
waiting time, not a bug.

## Resetting local state

Service B saves its total in `service-b/sum_state.json` and consumes with a
Kafka consumer group. To replay all retained events from the beginning, stop
Service B, remove the state file, and use a new consumer group ID:

```bash
cd service-b
rm sum_state.json
KAFKA_GROUP_ID=service-b-replay uvicorn main:app --host 0.0.0.0 --port 8000
```

Do not remove only the state file while keeping the same consumer group. The
consumer would continue from its old position and the total would be wrong.
The opposite is also a problem: reusing an old state file with a new group
counts retained events twice.

## Known gotchas

- **`db/init.sql` runs only once.** Postgres runs it only when its data
  volume is empty. To run it again, use `docker compose down -v`. This
  deletes **all** named volumes (`pgdata` and `grafana-data`), so keep your
  Grafana dashboard JSON in git.
- **The relay is required.** Service A does not publish to Kafka. Without the
  relay, events stay in the outbox with `published_at` set to `NULL`.
- **PostgreSQL must be ready before Service A starts.** If it is not,
  Service A fails when it inserts its first event.
- **Port `5433` for Postgres.** It avoids a conflict with a native Postgres
  install that may already use `5432`.
- **Single-node Kafka replication factor.** Kafka internal topics need 3
  replicas by default. With one broker this is overridden in
  `docker-compose.yml` with `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1` and
  related settings. Without it, consumers loop on
  `GroupCoordinatorNotAvailableError`.
- **Kafka listener addresses.** Kafka has separate `PLAINTEXT` (for other
  containers) and `EXTERNAL` (for host processes, port `9094`) listeners.
  When Service A, B, and the relay move into Docker, they must use
  `kafka:9092` instead of `localhost:9094`.
- **Prometheus reaches host services through `host.docker.internal`.** This
  works because the `prometheus` service in `docker-compose.yml` has
  `extra_hosts: ["host.docker.internal:host-gateway"]`. Once the services run
  in Docker, change the targets in `prometheus/prometheus.yml` to service
  names such as `service-a:9100`.
- **"network not found" from Docker Compose.** Usually stale Docker state.
  Run `docker compose down --remove-orphans`, then restart Docker if the
  error stays.

## Load testing

`load-test/grpc-test.js` is a [K6](https://k6.io/) script that sends gRPC
`Add` requests directly to Service A at increasing rates, so you can watch
the pipeline under real load instead of a few manual calls.

### Install K6

Follow the instructions for your OS at
https://grafana.com/docs/k6/latest/set-up/install-k6/. K6's gRPC support
(`k6/net/grpc`) is built in — no extra plugin needed. Confirm it installed:

```bash
k6 version
```

### What the script does

- Loads `adder.proto` and connects to Service A on `localhost:50051`.
- Uses the `ramping-arrival-rate` executor, which sends a fixed number of
  requests per second, ramping through stages: `10/s` for 30s, `25/s` for
  1m, `50/s` for 1m, then down to `0` over 10s. Total run time is about
  2m40s.
- Each virtual user connects once and reuses that connection, matching how
  a real client behaves.
- Fails the run if fewer than 99% of checks pass, or if `grpc_req_duration`
  p95 goes over 500ms.

### Clean up before running

Old data changes the results, and stopped services keep stale numbers in
memory. Before each run:

```bash
# 1. Stop Service A, the relay, and Service B (Ctrl+C each)

# 2. Empty the outbox
docker exec -it micro-services-postgres-1 psql -U anwar -d sumdb -c "TRUNCATE TABLE outbox;"

# 3. Delete the Kafka topic in Kafka UI (localhost:8080 -> Topics -> number-added -> delete)
#    It is recreated automatically on the next publish.

# 4. Reset Service B's saved total
rm service-b/sum_state.json

# 5. Start Service A, the relay, and Service B again
```

Confirm the reset worked before running K6:

```bash
curl -s http://localhost:9100/metrics | grep add_requests_total
# should print 0
```

### Run it

```bash
cd load-test
k6 run grpc-test.js
```

Open the Grafana dashboard in another tab first, with the time range set to
the last 15 minutes and auto-refresh on, so you can watch the latency and
CPU panels move while the test runs.

## Environment variables

| Service   | Variable            | Default           | Purpose                          |
| --------- | ------------------- | ----------------- | -------------------------------- |
| Service A | `GRPC_PORT`         | `50051`           | gRPC server port                 |
| Service A | `POSTGRES_HOST`     | `localhost`       | PostgreSQL host                  |
| Service A | `POSTGRES_PORT`     | `5433`            | PostgreSQL port                  |
| Service A | `POSTGRES_DB`       | `sumdb`           | PostgreSQL database              |
| Service A | `POSTGRES_USER`     | `anwar`           | PostgreSQL user                  |
| Service A | `POSTGRES_PASSWORD` | `1234`            | PostgreSQL password              |
| Relay     | `KAFKA_BROKER`      | `localhost:9094`  | Kafka bootstrap address          |
| Relay     | `KAFKA_TOPIC`       | `number-added`    | Topic to publish to              |
| Relay     | `POLL_INTERVAL_MS`  | `2000`            | Outbox polling interval          |
| Relay     | `POSTGRES_HOST`     | `localhost`       | PostgreSQL host                  |
| Relay     | `POSTGRES_PORT`     | `5433`            | PostgreSQL port                  |
| Relay     | `POSTGRES_DB`       | `sumdb`           | PostgreSQL database              |
| Relay     | `POSTGRES_USER`     | `anwar`           | PostgreSQL user                  |
| Relay     | `POSTGRES_PASSWORD` | `1234`            | PostgreSQL password              |
| Service B | `KAFKA_BROKER`      | `localhost:9094`  | Kafka bootstrap address          |
| Service B | `KAFKA_TOPIC`       | `number-added`    | Topic to consume from            |
| Service B | `KAFKA_GROUP_ID`    | `service-b-group` | Consumer group ID                |
| Service B | `SUM_FILE_PATH`     | `sum_state.json`  | Where the running total is saved |

The default credentials are for local development only. Do not reuse them
anywhere real.

## Roadmap

- Containerize Service A, the relay, and Service B so `docker compose up`
  starts everything
