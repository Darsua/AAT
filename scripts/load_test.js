import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Problem 2 load test (IF4031 M1): >= 50 parallel connections for 60 s, run together with a slow upstream.
//
// Scenarios (all run at the same time):
//   bmkg_only    25 VUs  GET /api/v1/hazards?type=SEISMIC   (needs BMKG data only)
//   volcanic     25 VUs  GET /api/v1/hazards?type=VOLCANIC  (needs PVMBG data)
//   pvmbg_direct  5 VUs  GET mock-pvmbg /volcanic-reports   (keeps the slow upstream busy; optional)
//
// Run it with the PVMBG delay fixed at 3 s to reproduce the Problem 2 criterion 1 setup:
//   PVMBG_DELAY_MIN_MS=3000 PVMBG_DELAY_MAX_MS=3000 docker compose up -d mock-pvmbg
//
// Each VU requests its own access token and renews it before the 60 s TTL ends, so the test is
// not affected by token expiry.

// Counters for every outcome so that failures can be explained by status code.
const status200 = new Counter('bnpb_status_200');
const status401 = new Counter('bnpb_status_401');
const status403 = new Counter('bnpb_status_403');
// Controlled rejections (HTTP 429) per spec: "Penolakan terkontrol seperti HTTP 429
// tidak dihitung sebagai kegagalan, selama jumlahnya dilaporkan".
const status429 = new Counter('bnpb_throttled_requests');
const status5xx = new Counter('bnpb_status_5xx');
const statusOther = new Counter('bnpb_status_other');
const errorRate = new Rate('bnpb_error_rate');

const bmkgOnlyLatency = new Trend('bnpb_bmkg_only_latency_ms', true);
const volcanicLatency = new Trend('bnpb_volcanic_latency_ms', true);
const pvmbgDirectLatency = new Trend('bnpb_pvmbg_direct_latency_ms', true);

// Only 200 and 429 are expected outcomes for k6's built-in http_req_failed metric.
http.setResponseCallback(http.expectedStatuses(200, 429));

const BASE_URL = __ENV.TARGET_URL || 'http://localhost:8080';
const AUTH_URL = __ENV.AUTH_URL || 'http://localhost:8084';
const PVMBG_URL = __ENV.PVMBG_URL || 'http://localhost:8082';
const PVMBG_TOKEN = __ENV.PVMBG_TOKEN || '';
const DURATION = __ENV.LOAD_DURATION || '60s';
const TOKEN_RENEW_MS = 40000; // renew well before the default 60 s access-token TTL

const scenarios = {
  bmkg_only: { executor: 'constant-vus', vus: 25, duration: DURATION, exec: 'bmkgOnly' },
  volcanic: { executor: 'constant-vus', vus: 25, duration: DURATION, exec: 'volcanic' },
};
if (PVMBG_TOKEN) {
  scenarios.pvmbg_direct = { executor: 'constant-vus', vus: 5, duration: DURATION, exec: 'pvmbgDirect' };
}

export const options = {
  scenarios,
  summaryTrendStats: ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"],
  thresholds: {
    // Problem 2 criterion 1: BMKG-only p95 stays below 300 ms while PVMBG is slow.
    bnpb_bmkg_only_latency_ms: ['p(95)<300'],
    bnpb_volcanic_latency_ms: ['p(95)<300'],
    // Problem 2 criterion 2: error rate (excluding controlled 429 rejections) below 1%.
    bnpb_error_rate: ['rate<0.01'],
    http_req_failed: ['rate<0.01'],
  },
};

export function setup() {
  if (!__ENV.AUTH_INTERNAL_CLIENT_SECRET) {
    throw new Error('AUTH_INTERNAL_CLIENT_SECRET environment variable is required');
  }
  if (!PVMBG_TOKEN) {
    console.warn('PVMBG_TOKEN is not set: the pvmbg_direct scenario is skipped');
  }
}

// Per-VU access token cache (each VU has its own module state).
let accessToken = '';
let issuedAt = 0;

function getToken() {
  const now = Date.now();
  if (accessToken && now - issuedAt < TOKEN_RENEW_MS) {
    return accessToken;
  }
  const res = http.post(
    `${AUTH_URL}/auth/token`,
    JSON.stringify({
      client_id: __ENV.AUTH_INTERNAL_CLIENT_ID || 'test-internal-client',
      client_secret: __ENV.AUTH_INTERNAL_CLIENT_SECRET,
      requested_scope: 'internal',
    }),
    { headers: { 'Content-Type': 'application/json' }, timeout: '5s', tags: { name: 'auth_token' } },
  );
  if (res.status === 200) {
    accessToken = JSON.parse(res.body).access_token;
    issuedAt = now;
  }
  return accessToken;
}

function recordStatus(status) {
  if (status === 200) status200.add(1);
  else if (status === 401) status401.add(1);
  else if (status === 403) status403.add(1);
  else if (status === 429) status429.add(1);
  else if (status >= 500) status5xx.add(1);
  else statusOther.add(1);
}

function queryGateway(type, trend) {
  const params = {
    headers: {
      Accept: 'application/json',
      Authorization: `Bearer ${getToken()}`,
      'X-Correlation-ID': `load-${type}-${__VU}-${__ITER}-${Date.now()}`,
    },
    timeout: '5s',
    tags: { name: `hazards_${type}` },
  };
  const res = http.get(`${BASE_URL}/api/v1/hazards?limit=20&fields=summary&type=${type}`, params);
  trend.add(res.timings.duration);
  recordStatus(res.status);

  if (res.status !== 429) {
    const ok = check(res, {
      'status is 200': (r) => r.status === 200,
      'response has valid content': (r) => r.body && r.body.length > 0,
    });
    errorRate.add(!ok);
  }
  sleep(0.05 + Math.random() * 0.1); // pacing jitter 50 to 150 ms
}

export function bmkgOnly() {
  queryGateway('SEISMIC', bmkgOnlyLatency);
}

export function volcanic() {
  queryGateway('VOLCANIC', volcanicLatency);
}

export function pvmbgDirect() {
  const res = http.get(`${PVMBG_URL}/volcanic-reports`, {
    headers: { Authorization: `Bearer ${PVMBG_TOKEN}` },
    timeout: '10s',
    tags: { name: 'pvmbg_direct' },
  });
  pvmbgDirectLatency.add(res.timings.duration);
  check(res, { 'pvmbg direct returns 200': (r) => r.status === 200 });
  sleep(0.1);
}
