const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const pg = require('pg');
const client = require('@prometheus-io/client');
const http = require('http');

client.collectDefaultMetrics();
const addCounter = new client.Counter({
  name: 'add_requests_total',
  help: 'Total number of Add calls',
});

const PROTO_PATH = path.join(__dirname, '..', 'proto', 'adder.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const adderProto = grpc.loadPackageDefinition(packageDefinition).adder;

const pool = new pg.Pool({
  user: process.env.POSTGRES_USER || 'anwar',
  host: process.env.POSTGRES_HOST || 'localhost',
  database: process.env.POSTGRES_DB || 'sumdb',
  password: process.env.POSTGRES_PASSWORD || '1234',
  port: parseInt(process.env.POSTGRES_PORT, 10) || 5433,
});

// --- RPC handler ---
// Adds the two numbers, then writes the result to the outbox table.
// We await the write before calling back so a DB failure is visible
// to the caller as an error, rather than silently dropped.
async function add(call, callback) {
  const { a, b } = call.request;

  if (
    typeof a !== 'number' ||
    typeof b !== 'number' ||
    Number.isNaN(a) ||
    Number.isNaN(b)
  ) {
    return callback({
      code: grpc.status.INVALID_ARGUMENT,
      message: "Both 'a' and 'b' must be valid numbers",
    });
  }

  const sum = a + b;
  console.log(`[Add] ${a} + ${b} = ${sum}`);

  let eventId;
  try {
    const result = await pool.query(
      `INSERT INTO outbox (event_type, payload) VALUES ($1, $2) RETURNING id`,
      [
        'NumberAdded',
        JSON.stringify({ value: sum, produced_at: new Date().toISOString() }),
      ],
    );
    eventId = result.rows[0].id;
    console.log(`[Outbox] wrote event ${eventId}`);
    addCounter.inc(); // Increment the Prometheus counter for successful add calls
  } catch (err) {
    console.error(`[Outbox] failed to write:`, err.message);
    return callback({
      code: grpc.status.UNAVAILABLE,
      message: `Computed sum but failed to write outbox event: ${err.message}`,
    });
  }

  callback(null, { sum, event_id: eventId });
}

async function main() {
  const server = new grpc.Server();
  server.addService(adderProto.AdderService.service, { add });

  const port = process.env.GRPC_PORT || '50051';
  const bindAddr = `0.0.0.0:${port}`;

  server.bindAsync(bindAddr, grpc.ServerCredentials.createInsecure(), () => {
    console.log(`Service A (gRPC) listening on ${bindAddr}`);
  });

  // Start the Prometheus metrics server
  http
    .createServer(async (req, res) => {
      res.setHeader('Content-Type', client.register.contentType);
      res.end(await client.register.metrics());
    })
    .listen(9100, () => {
      console.log('Metrics server listening on 0.0.0.0:9100');
    });
}

main();
