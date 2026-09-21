const path = require('path');
const crypto = require('crypto');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka } = require('kafkajs');

const PROTO_PATH = path.join(__dirname, '..', 'proto', 'adder.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const adderProto = grpc.loadPackageDefinition(packageDefinition).adder;

// --- Kafka producer setup ---
// KAFKA_BROKER defaults to Kafka's "EXTERNAL" listener (localhost:9094)
// because we're running this process directly on the host, not inside Docker.
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9094';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'number-added';

const kafka = new Kafka({
  clientId: 'service-a',
  brokers: [KAFKA_BROKER],
});
const producer = kafka.producer();

// --- RPC handler ---
// Adds the two numbers, then publishes the result to Kafka.
// We await the Kafka publish before calling back so a publish failure
// is visible to the caller as an error, rather than silently dropped.
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
  const eventId = crypto.randomUUID();

  console.log(`[Add] ${a} + ${b} = ${sum}`);

  try {
    await producer.send({
      topic: KAFKA_TOPIC,
      messages: [
        {
          key: eventId,
          value: JSON.stringify({
            event_id: eventId,
            value: sum,
            produced_at: new Date().toISOString(),
          }),
        },
      ],
    });
    console.log(`[Kafka] published event ${eventId} to ${KAFKA_TOPIC}`);
  } catch (err) {
    console.error(`[Kafka] failed to publish:`, err.message);
    return callback({
      code: grpc.status.UNAVAILABLE,
      message: `Computed sum but failed to publish to Kafka: ${err.message}`,
    });
  }

  callback(null, { sum, event_id: eventId });
}

async function main() {
  await producer.connect();
  console.log(`[Kafka] producer connected to ${KAFKA_BROKER}`);

  const server = new grpc.Server();
  server.addService(adderProto.AdderService.service, { add });

  const port = process.env.GRPC_PORT || '50051';
  const bindAddr = `0.0.0.0:${port}`;

  server.bindAsync(bindAddr, grpc.ServerCredentials.createInsecure(), () => {
    console.log(`Service A (gRPC) listening on ${bindAddr}`);
  });
}

// Graceful shutdown: disconnect the producer cleanly rather than just dying
process.on('SIGINT', async () => {
  console.log('\nShutting down, disconnecting Kafka producer...');
  await producer.disconnect();
  process.exit(0);
});

main();
