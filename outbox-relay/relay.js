const { Kafka } = require('kafkajs');
const pg = require('pg');
const client = require('@prometheus-io/client');
const http = require('http');

// Prometheus metrics
const publishLatency = new client.Histogram({
  name: 'outbox_publish_latency_seconds',
  help: 'Latency of publishing outbox events to Kafka',
  buckets: [0.01, 0.05, 0.1, 0.5, 1, 2, 5],
});

// --- Kafka producer setup ---
// KAFKA_BROKER defaults to Kafka's "EXTERNAL" listener (localhost:9094)
// because we're running this process directly on the host, not inside Docker.
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9094';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'number-added';

const kafka = new Kafka({
  clientId: 'outbox-relay',
  brokers: [KAFKA_BROKER],
});

const producer = kafka.producer();

// --- Postgres pool setup ---
// Same connection details as service-a/server.js, since both talk to
// the same outbox table.
const pool = new pg.Pool({
  user: process.env.POSTGRES_USER || 'anwar',
  host: process.env.POSTGRES_HOST || 'localhost',
  database: process.env.POSTGRES_DB || 'sumdb',
  password: process.env.POSTGRES_PASSWORD || '1234',
  port: parseInt(process.env.POSTGRES_PORT, 10) || 5433,
});

const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS, 10) || 2000;
const BATCH_SIZE = 50;

// A simple flag so we don't start a second poll cycle while one is still
// running (e.g. if a poll cycle takes longer than POLL_INTERVAL_MS due to
// a slow Kafka broker) — avoids overlapping cycles processing the same rows.
let isPolling = false;

async function pollAndPublish() {
  if (isPolling) return;
  isPolling = true;

  try {
    const { rows } = await pool.query(
      `SELECT id, payload, created_at FROM outbox
       WHERE published_at IS NULL
       ORDER BY created_at ASC
       LIMIT $1`,
      [BATCH_SIZE],
    );

    if (rows.length === 0) {
      return; // nothing pending, nothing to do this cycle
    }

    console.log(`[relay] found ${rows.length} unpublished event(s)`);

    for (const row of rows) {
      try {
        await producer.send({
          topic: KAFKA_TOPIC,
          messages: [
            {
              key: row.id,
              value: JSON.stringify({
                event_id: row.id,
                value: row.payload.value,
                produced_at: row.payload.produced_at,
              }),
            },
          ],
        });

        await pool.query(
          `UPDATE outbox SET published_at = now() WHERE id = $1`,
          [row.id],
        );

        // Record the latency from when the event was created to when it was published.
        const latencySeconds =
          (Date.now() - new Date(row.created_at).getTime()) / 1000;
        publishLatency.observe(latencySeconds);

        console.log(`[relay] published and marked event ${row.id}`);
      } catch (err) {
        // One bad row shouldn't block the rest of the batch — log it and
        // move on. Since published_at is still NULL, this row will simply
        // be retried on the next poll cycle.
        console.error(
          `[relay] failed to publish event ${row.id}:`,
          err.message,
        );
      }
    }
  } catch (err) {
    // The SELECT itself failed (e.g. Postgres unreachable) — nothing to do
    // but wait for the next cycle to try again.
    console.error(`[relay] poll cycle failed:`, err.message);
  } finally {
    isPolling = false;
  }
}

async function main() {
  await producer.connect();
  console.log(`[relay] Kafka producer connected to ${KAFKA_BROKER}`);
  console.log(`[relay] polling outbox every ${POLL_INTERVAL_MS}ms`);

  setInterval(pollAndPublish, POLL_INTERVAL_MS);

  // Start the Prometheus metrics server
  http
    .createServer(async (req, res) => {
      res.setHeader('Content-Type', client.register.contentType);
      res.end(await client.register.metrics());
    })
    .listen(9101, () => {
      console.log('Metrics server listening on 0.0.0.0:9101');
    });
}

process.on('SIGINT', async () => {
  console.log('\n[relay] shutting down, disconnecting Kafka producer...');
  await producer.disconnect();
  await pool.end();
  process.exit(0);
});

main();
