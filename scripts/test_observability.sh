#!/usr/bin/env bash
# ==============================================================================
# Correlation IDs, Latency Logs, Health Checks, and Broker Metrics (malas, nanti cek lagi)
# Demonstrates:
#   1. Auth, Client API, Aggregator, and Redpanda health.
#   2. X-Correlation-ID propagation from Client API to Aggregator.
#   3. Request latency and status fields in both HTTP service logs.
#   4. Kafka correlation headers from a direct test event reaching both consumers.
#   5. Consumer-group lag as an operational broker metric.
# This script audits the HTTP and Kafka paths separately; it does not claim to
# exercise the Aggregator transactional-outbox publisher end to end.
# ==============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

AUTH_URL="${AUTH_URL:-http://localhost:8084}"
CLIENT_API_URL="${CLIENT_API_URL:-http://localhost:8080}"
TOPIC="${TOPIC:-bnpb.hazard-events}"
INTERNAL_CLIENT_ID="${AUTH_INTERNAL_CLIENT_ID:-test-internal-client}"
INTERNAL_CLIENT_SECRET="${AUTH_INTERNAL_CLIENT_SECRET:-}"
CONSUMER_1_SERVICE="${CONSUMER_1_SERVICE:-dashboard-consumer}"
CONSUMER_1_GROUP="${CONSUMER_1_GROUP:-bnpb-dashboard-group}"
CONSUMER_2_SERVICE="${CONSUMER_2_SERVICE:-notifier-consumer}"
CONSUMER_2_GROUP="${CONSUMER_2_GROUP:-bnpb-notifier-group}"
DOCKER_COMPOSE="${DOCKER_COMPOSE:-}"

PASSES=0
FAILURES=0
RUN_ID="$(date +%s)-$$"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TEMP_DIR}"' EXIT

pass() {
    PASSES=$((PASSES + 1))
    echo -e "${GREEN}[PASS]${NC} $1"
}

fail() {
    FAILURES=$((FAILURES + 1))
    echo -e "${RED}[FAIL]${NC} $1" >&2
}

fatal() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
    exit 1
}

# Log writes are asynchronous relative to curl and Kafka delivery.
wait_for_log() {
    local service="$1"
    local value="$2"

    for _ in {1..15}; do
        if ${DOCKER_COMPOSE} logs --no-color "${service}" 2>/dev/null | grep -Fq "${value}"; then
            return 0
        fi
        sleep 1
    done
    return 1
}

command -v curl >/dev/null 2>&1 || fatal "Required command 'curl' is not installed"
command -v jq >/dev/null 2>&1 || fatal "Required command 'jq' is not installed"
command -v docker >/dev/null 2>&1 || fatal "Required command 'docker' is not installed"
[[ -n "${INTERNAL_CLIENT_SECRET}" ]] || fatal "AUTH_INTERNAL_CLIENT_SECRET must be set"

if [[ -z "${DOCKER_COMPOSE}" ]]; then
    if docker info >/dev/null 2>&1; then
        DOCKER_COMPOSE="docker compose"
    else
        DOCKER_COMPOSE="sudo docker compose"
    fi
fi

echo -e "${BLUE}========================================================================${NC}"
echo -e "${CYAN}        C-09: Correlation, Latency, Health & Broker Metrics Audit       ${NC}"
echo -e "${BLUE}========================================================================${NC}"

echo -e "\n${BLUE}[Step 0] Checking service and broker health...${NC}"
curl --silent --show-error --fail "${AUTH_URL}/health" | jq .
pass "Auth health endpoint is available."
curl --silent --show-error --fail "${CLIENT_API_URL}/health" | jq .
pass "Client API health endpoint is available."
AGGREGATOR_HEALTH="$(${DOCKER_COMPOSE} exec -T aggregator wget -qO- http://localhost:8083/health)" ||
    fatal "Aggregator health endpoint is unavailable inside its container"
jq . <<<"${AGGREGATOR_HEALTH}"
pass "Aggregator health endpoint reports its dependencies."
if ${DOCKER_COMPOSE} exec -T redpanda rpk cluster info >/dev/null 2>&1; then
    pass "Redpanda cluster is responsive."
else
    fatal "Redpanda cluster is unavailable"
fi

echo -e "\n${BLUE}[Step 1] Requesting an Internal access token...${NC}"
TOKEN_PAYLOAD="$(jq -cn \
    --arg client_id "${INTERNAL_CLIENT_ID}" \
    --arg client_secret "${INTERNAL_CLIENT_SECRET}" \
    '{client_id: $client_id, client_secret: $client_secret, requested_scope: "internal"}')"
TOKEN_STATUS="$(curl --silent --show-error \
    --output "${TEMP_DIR}/token.json" \
    --write-out '%{http_code}' \
    --request POST "${AUTH_URL}/auth/token" \
    --header 'Content-Type: application/json' \
    --data "${TOKEN_PAYLOAD}")"
if [[ "${TOKEN_STATUS}" != "200" ]]; then
    fatal "Internal token request returned HTTP ${TOKEN_STATUS}: $(<"${TEMP_DIR}/token.json")"
fi
ACCESS_TOKEN="$(jq -er '.access_token' "${TEMP_DIR}/token.json")" ||
    fatal "Auth response does not contain access_token"
pass "Internal access token was issued."

echo -e "\n${BLUE}[Step 2] Sending a traced request through Client API...${NC}"
HTTP_CORRELATION_ID="c09-http-${RUN_ID}"
HTTP_STATUS="$(curl --silent --show-error \
    --dump-header "${TEMP_DIR}/headers.txt" \
    --output "${TEMP_DIR}/hazards.json" \
    --write-out '%{http_code}' \
    "${CLIENT_API_URL}/api/v1/hazards?fields=all&limit=1" \
    --header "Authorization: Bearer ${ACCESS_TOKEN}" \
    --header "X-Correlation-ID: ${HTTP_CORRELATION_ID}")"
echo "HTTP Status Code: ${HTTP_STATUS}"
if [[ "${HTTP_STATUS}" == "200" ]]; then
    pass "Traced Client API request returned HTTP 200."
else
    fail "Traced Client API request returned HTTP ${HTTP_STATUS}: $(<"${TEMP_DIR}/hazards.json")"
fi

RESPONSE_CORRELATION_ID="$(awk 'tolower($1) == "x-correlation-id:" { gsub("\r", "", $2); print $2 }' "${TEMP_DIR}/headers.txt" | tail -n 1)"
if [[ "${RESPONSE_CORRELATION_ID}" == "${HTTP_CORRELATION_ID}" ]]; then
    pass "Client API returned the original X-Correlation-ID."
else
    fail "Expected response correlation ID '${HTTP_CORRELATION_ID}', got '${RESPONSE_CORRELATION_ID:-empty}'."
fi

echo -e "\n${BLUE}[Step 3] Verifying the HTTP correlation and latency log trail...${NC}"
if wait_for_log client-api "${HTTP_CORRELATION_ID}"; then
    CLIENT_LOG="$(${DOCKER_COMPOSE} logs --no-color client-api 2>/dev/null | grep -F "${HTTP_CORRELATION_ID}" | tail -n 1)"
    echo "${CLIENT_LOG}"
    if grep -Fq 'duration_ms=' <<<"${CLIENT_LOG}"; then
        pass "Client API log contains correlation ID and request latency."
    else
        fail "Client API trace log is missing duration_ms."
    fi
else
    fail "Client API log does not contain '${HTTP_CORRELATION_ID}'."
fi

if wait_for_log aggregator "${HTTP_CORRELATION_ID}"; then
    AGGREGATOR_LOG="$(${DOCKER_COMPOSE} logs --no-color aggregator 2>/dev/null | grep -F "${HTTP_CORRELATION_ID}" | tail -n 1)"
    echo "${AGGREGATOR_LOG}"
    if grep -Fq 'duration_ms' <<<"${AGGREGATOR_LOG}"; then
        pass "Aggregator received the same correlation ID and logged latency."
    else
        fail "Aggregator trace log is missing duration_ms."
    fi
else
    fail "Aggregator log does not contain '${HTTP_CORRELATION_ID}'."
fi

echo -e "\n${BLUE}[Step 4] Verifying automatic correlation-ID generation...${NC}"
GENERATED_STATUS="$(curl --silent --show-error \
    --dump-header "${TEMP_DIR}/generated-headers.txt" \
    --output /dev/null \
    --write-out '%{http_code}' \
    "${CLIENT_API_URL}/api/v1/hazards?fields=all&limit=1" \
    --header "Authorization: Bearer ${ACCESS_TOKEN}")"
GENERATED_CORRELATION_ID="$(awk 'tolower($1) == "x-correlation-id:" { gsub("\r", "", $2); print $2 }' "${TEMP_DIR}/generated-headers.txt" | tail -n 1)"
if [[ "${GENERATED_STATUS}" == "200" && -n "${GENERATED_CORRELATION_ID}" ]]; then
    pass "Client API generated X-Correlation-ID '${GENERATED_CORRELATION_ID}'."
else
    fail "Client API did not generate a correlation ID; HTTP ${GENERATED_STATUS}."
fi

echo -e "\n${BLUE}[Step 5] Producing a traced Kafka test event for both consumers...${NC}"
${DOCKER_COMPOSE} up -d "${CONSUMER_1_SERVICE}" "${CONSUMER_2_SERVICE}" >/dev/null
KAFKA_CORRELATION_ID="c09-kafka-${RUN_ID}"
KAFKA_EVENT_ID="c09-event-${RUN_ID}"
KAFKA_EVENT="{\"hazard_id\":\"${KAFKA_EVENT_ID}\",\"source\":\"BMKG\",\"source_ref_id\":\"${KAFKA_EVENT_ID}\",\"hazard_type\":\"SEISMIC\",\"severity\":\"SIAGA\",\"area_name\":\"C-09 Trace Test\",\"occurred_at\":\"2026-10-06T00:03:00Z\",\"ingested_at\":\"2026-10-06T00:03:01Z\",\"attributes\":{\"magnitude\":6.7}}"
printf '%s\n' "${KAFKA_EVENT}" |
    ${DOCKER_COMPOSE} exec -T redpanda rpk topic produce "${TOPIC}" \
        -k "${KAFKA_EVENT_ID}" \
        -H "X-Correlation-ID:${KAFKA_CORRELATION_ID}" >/dev/null

if wait_for_log "${CONSUMER_1_SERVICE}" "${KAFKA_CORRELATION_ID}"; then
    ${DOCKER_COMPOSE} logs --no-color "${CONSUMER_1_SERVICE}" 2>/dev/null |
        grep -F "${KAFKA_CORRELATION_ID}" | tail -n 1
    pass "Dashboard logged the Kafka correlation ID."
else
    fail "Dashboard did not log Kafka correlation ID '${KAFKA_CORRELATION_ID}'."
fi
if wait_for_log "${CONSUMER_2_SERVICE}" "${KAFKA_CORRELATION_ID}"; then
    ${DOCKER_COMPOSE} logs --no-color "${CONSUMER_2_SERVICE}" 2>/dev/null |
        grep -F "${KAFKA_CORRELATION_ID}" | tail -n 1
    pass "Notifier logged the same Kafka correlation ID."
else
    fail "Notifier did not log Kafka correlation ID '${KAFKA_CORRELATION_ID}'."
fi

echo -e "\n${BLUE}[Step 6] Reading consumer-group lag metrics...${NC}"
DASHBOARD_METRICS="$(${DOCKER_COMPOSE} exec -T redpanda rpk group describe "${CONSUMER_1_GROUP}" --print-summary 2>/dev/null || true)"
NOTIFIER_METRICS="$(${DOCKER_COMPOSE} exec -T redpanda rpk group describe "${CONSUMER_2_GROUP}" --print-summary 2>/dev/null || true)"
echo "${DASHBOARD_METRICS}"
echo "${NOTIFIER_METRICS}"
DASHBOARD_LAG="$(awk '$1 == "TOTAL-LAG" { print $2 }' <<<"${DASHBOARD_METRICS}")"
NOTIFIER_LAG="$(awk '$1 == "TOTAL-LAG" { print $2 }' <<<"${NOTIFIER_METRICS}")"
if [[ "${DASHBOARD_LAG}" =~ ^[0-9]+$ && "${NOTIFIER_LAG}" =~ ^[0-9]+$ ]]; then
    pass "Broker reports numeric lag for both consumer groups."
else
    fail "Unable to read numeric consumer-group lag metrics."
fi

echo -e "\n${BLUE}Results:${NC} ${PASSES} passed, ${FAILURES} failed"
if ((FAILURES > 0)); then
    exit 1
fi

echo -e "\n${GREEN}========================================================================${NC}"
echo -e "${GREEN}          Verification Complete: Observability Trail Verified!          ${NC}"
echo -e "${GREEN}========================================================================${NC}"
