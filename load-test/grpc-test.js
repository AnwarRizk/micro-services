import grpc from 'k6/net/grpc';
import { check } from 'k6';

const client = new grpc.Client();
client.load(['../proto'], 'adder.proto');

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: 5,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 300,
      stages: [
        { target: 20, duration: '30s' }, // warm up, well under old ceiling
        { target: 50, duration: '1m' }, // the OLD ceiling — should now be flat, not climbing
        { target: 100, duration: '1m' }, // the NEW predicted ceiling
        { target: 150, duration: '1m' }, // push past the new ceiling on purpose
        { target: 0, duration: '10s' }, // ramp down
      ],
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    grpc_req_duration: ['p(95)<500'],
  },
};

export default () => {
  // Each virtual user connects once, on its first iteration, and reuses
  // that connection for every call after — matching how a real client
  // would behave, instead of reconnecting every time.
  if (__ITER === 0) {
    client.connect('localhost:50051', { plaintext: true });
  }

  const response = client.invoke('adder.AdderService/Add', {
    a: Math.random() * 100,
    b: Math.random() * 100,
  });

  check(response, {
    'status is OK': (r) => r && r.status === grpc.StatusOK,
  });
};
