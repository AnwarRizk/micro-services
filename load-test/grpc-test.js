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
      maxVUs: 200,
      stages: [
        { target: 10, duration: '30s' }, // warm up
        { target: 25, duration: '1m' }, // around the predicted relay limit
        { target: 50, duration: '1m' }, // push past it
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
