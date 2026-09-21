const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PROTO_PATH = path.join(__dirname, '..', 'proto', 'adder.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const adderProto = grpc.loadPackageDefinition(packageDefinition).adder;

const target = process.env.GRPC_TARGET || 'localhost:50051';

// Create a gRPC client for the AdderService
const client = new adderProto.AdderService(
  target,
  grpc.credentials.createInsecure(),
);

// Get the numbers to add from command line arguments or use defaults
const a = Number(process.argv[2] ?? 3);
const b = Number(process.argv[3] ?? 4);

client.add({ a, b }, (err, response) => {
  if (err) {
    console.error('Error calling Add:', err.message);
    process.exit(1);
  }
  console.log(`Result: ${a} + ${b} = ${response.sum}`);
  process.exit(0);
});
