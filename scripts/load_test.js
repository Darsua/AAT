import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom metrics
const successfulRequests = new Counter('bnpb_successful_requests');
const failedRequests = new Counter('bnpb_failed_requests');
// Controlled rejections (HTTP 429) per spec: "Penolakan terkontrol seperti HTTP 429
// tidak dihitung sebagai kegagalan, selama jumlahnya dilaporkan" -- tracked separately
// so they are visible in results but never counted toward the error-rate threshold.
const throttledRequests = new Counter('bnpb_throttled_requests');
const hazardLatencyTrend = new Trend('bnpb_hazard_latency_ms');
const errorRate = new Rate('bnpb_error_rate');

// Treat only 200 and 429 as "expected" outcomes for k6's built-in http_req_failed
// metric. 429 is a controlled rejection (load shedding), not a service failure, and
// must not count against the failure-rate threshold per the M1 spec.
http.setResponseCallback(http.expectedStatuses(200, 429));

// Test Configuration (Aligned with IF4031 M1 Problem 2 spec: >=50 connections, >=60s sustained, error rate <1%)
export const options = {
  vus: 50,
  duration: '60s',
  thresholds: {
    // p95 latency under 300ms, p99 under 500ms
    http_req_duration: ['p(95)<300', 'p(99)<500'],
    // Error rate must remain strictly under 1% per course spec.
    // Controlled rejections (429) are excluded via expectedStatuses() above and via
    // the custom bnpb_error_rate metric logic below -- both only count true failures.
    http_req_failed: ['rate<0.01'],
    bnpb_error_rate: ['rate<0.01'],
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://localhost:8080';
const AUTH_URL = __ENV.AUTH_URL || 'http://localhost:8084';

export function setup() {
  // Try to obtain a valid auth token if auth service is up
  let token = 'mock-loadtest-internal-token';
  try {
    const authPayload = JSON.stringify({
      client_id: __ENV.AUTH_INTERNAL_CLIENT_ID || 'test-internal-client',
      client_secret: __ENV.AUTH_INTERNAL_CLIENT_SECRET || 'secret123',
      requested_scope: 'internal',
      role: 'internal',
    });
    const authRes = http.post(`${AUTH_URL}/auth/token`, authPayload, {
      headers: { 'Content-Type': 'application/json' },
      timeout: '3s',
    });
    if (authRes.status === 200) {
      const body = JSON.parse(authRes.body);
      if (body.access_token) {
        token = body.access_token;
      }
    }
  } catch (e) {
    // Fallback to mock token
  }
  return { token: token };
}

export default function (data) {
  // Query varying parameters to simulate realistic client traffic
  const queryTypes = ['SEISMIC', 'VOLCANIC', 'ALL'];
  const selectedType = queryTypes[Math.floor(Math.random() * queryTypes.length)];
  
  let endpoint = `${BASE_URL}/api/v1/hazards?limit=20&fields=summary`;
  if (selectedType !== 'ALL') {
    endpoint += `&type=${selectedType}`;
  }

  const params = {
    headers: {
      'Accept': 'application/json',
      'Authorization': `Bearer ${data.token}`,
      'X-Correlation-ID': `load-${__VU}-${__ITER}-${Date.now()}`,
    },
    timeout: '5s',
  };

  const start = Date.now();
  const res = http.get(endpoint, params);
  const latency = Date.now() - start;

  hazardLatencyTrend.add(latency);

  if (res.status === 429) {
    // Controlled rejection (load shedding) -- reported separately, not a failure.
    throttledRequests.add(1);
    check(res, { 'status is 429 (controlled rejection)': (r) => r.status === 429 });
  } else {
    const passed = check(res, {
      'status is 200': (r) => r.status === 200,
      'response has valid content': (r) => r.body && r.body.length > 0,
      'latency is under 300ms': () => latency < 300,
    });

    if (passed) {
      successfulRequests.add(1);
      errorRate.add(0);
    } else {
      failedRequests.add(1);
      errorRate.add(1);
    }
  }

  // Pacing jitter between requests (50ms - 150ms)
  sleep(0.05 + Math.random() * 0.1);
}
