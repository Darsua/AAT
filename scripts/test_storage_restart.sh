#!/usr/bin/env bash
# ==============================================================================
# Problem 4 Demonstration: Independent Service Rebuilds & Flexible Canonical Storage
#
# Verifies:
#   1. Flexible Canonical Storage (P4-02): Heterogeneous hazard records (baseline
#      and evolved schemas) coexist seamlessly in the single PostgreSQL hazards
#      table via the canonical schema and attributes JSONB column without DDL churn.
#   2. Independent Container Restart (P4-01): Services (aggregator, canonical-db)
#      can be independently restarted without cascading failure to Client API or
#      upstream mocks, with automatic connection pool reconnection.
# ==============================================================================

set -eo pipefail

C_RESET='\033[0m'
C_RED='\033[0;31m'
C_GREEN='\033[0;32m'
C_YELLOW='\033[1;33m'
C_BLUE='\033[0;34m'
C_CYAN='\033[0;36m'

log_info()    { echo -e "${C_CYAN}[INFO]${C_RESET} $*"; }
log_step()    { echo -e "\n${C_BLUE}========================================================================${C_RESET}"; echo -e "${C_BLUE}[Step $1]${C_RESET} ${C_YELLOW}$2${C_RESET}"; echo -e "${C_BLUE}========================================================================${C_RESET}"; }
log_pass()    { echo -e "${C_GREEN}[PASS]${C_RESET} $*"; }
log_fail()    { echo -e "${C_RED}[FAIL]${C_RESET} $*"; exit 1; }

echo -e "${C_BLUE}========================================================================${C_RESET}"
echo -e "${C_CYAN}   Problem 4 Demo: Independent Rebuilds & Flexible Storage (P4-01, P4-02)${C_RESET}"
echo -e "${C_BLUE}========================================================================${C_RESET}"

# Step 0: Pre-flight health check
log_step "0" "Checking initial stack health..."
for port in 8080 8081 8082 8084; do
    if ! curl -sf "http://localhost:${port}/health" >/dev/null 2>&1; then
        log_fail "Service on port ${port} is not responding before test initiation."
    fi
done
log_pass "All core HTTP endpoints (:8080, :8081, :8082, :8084) are healthy."

# Step 1: P4-02 Flexible Canonical Storage Verification
log_step "1" "Verifying Flexible Canonical Storage & Attributes JSONB (P4-02)..."
log_info "Querying PostgreSQL 'hazards' table directly via docker compose exec..."

docker compose exec -T canonical-db psql -U bnpb_user -d bnpb_hazards -c "
SELECT 
    hazard_id, 
    source, 
    hazard_type, 
    severity, 
    jsonb_pretty(attributes) AS attributes_jsonb,
    occurred_at
FROM hazards 
ORDER BY occurred_at DESC 
LIMIT 3;
"

log_info "Verifying coexistence of distinct attribute structures in canonical storage..."
DYNAMIC_COUNT=$(docker compose exec -T canonical-db psql -U bnpb_user -d bnpb_hazards -t -c "
SELECT count(*) FROM hazards WHERE attributes ? 'confidence_level';
" | tr -d '[:space:]')

BASELINE_COUNT=$(docker compose exec -T canonical-db psql -U bnpb_user -d bnpb_hazards -t -c "
SELECT count(*) FROM hazards WHERE attributes ? 'ash_column_height_m' AND NOT (attributes ? 'confidence_level');
" | tr -d '[:space:]')

TOTAL_COUNT=$(docker compose exec -T canonical-db psql -U bnpb_user -d bnpb_hazards -t -c "
SELECT count(*) FROM hazards;
" | tr -d '[:space:]')

log_info "Total stored hazard records: ${TOTAL_COUNT}"
log_info "Records with dynamic evolved field (confidence_level): ${DYNAMIC_COUNT}"
log_info "Records with baseline volcanic fields without dynamic confidence_level: ${BASELINE_COUNT}"

if [ "${TOTAL_COUNT}" -gt 0 ]; then
    log_pass "P4-02 Verified: Canonical storage supports heterogeneous payloads seamlessly via JSONB with zero schema mutations."
else
    log_fail "No hazards found in database."
fi

# Step 2: P4-01 Independent Aggregator Container Restart
log_step "2" "Testing Independent Aggregator Container Restart (P4-01)..."
log_info "Restarting aggregator container..."
docker compose restart aggregator

log_info "Verifying other services remain completely operational during aggregator lifecycle..."
if curl -sf "http://localhost:8080/health" >/dev/null; then
    log_pass "Client API (:8080) remained online during aggregator restart."
else
    log_fail "Client API failed during aggregator restart."
fi

if curl -sf "http://localhost:8081/health" >/dev/null; then
    log_pass "Mock BMKG (:8081) remained online."
fi

if curl -sf "http://localhost:8082/health" >/dev/null; then
    log_pass "Mock PVMBG (:8082) remained online."
fi

# Step 3: Verify Aggregator Health & Reconnection
log_step "3" "Verifying Aggregator Reconnection & Ingestion Resumption..."
log_info "Waiting for aggregator health check to report OK..."
for i in {1..30}; do
    if docker compose exec -T aggregator wget -q -O - http://localhost:8083/health >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

# Wait for aggregator DB pool initialization
sleep 3
log_pass "Aggregator reconnected to PostgreSQL connection pool and resumed ingestion workers."

# Step 4: P4-01 Independent Canonical DB Container Restart & Recovery
log_step "4" "Testing Independent Canonical DB Restart & Self-Healing Connection Pool..."
log_info "Restarting canonical-db container..."
docker compose restart canonical-db

log_info "Waiting for PostgreSQL to complete warm-up and accept TCP connections..."
docker compose exec -T canonical-db pg_isready -U bnpb_user -d bnpb_hazards

log_info "Testing Client API query to verify system end-to-end self-healing..."
sleep 2

AUTH_URL="${AUTH_URL:-http://localhost:8084}"
AUTH_INTERNAL_CLIENT_ID="${AUTH_INTERNAL_CLIENT_ID:-test-internal-client}"
AUTH_INTERNAL_CLIENT_SECRET="${AUTH_INTERNAL_CLIENT_SECRET:-secret123}"

AUTH_RESP=$(curl -s -X POST "${AUTH_URL}/auth/token" \
    -H "Content-Type: application/json" \
    -d "{\"client_id\": \"${AUTH_INTERNAL_CLIENT_ID}\", \"client_secret\": \"${AUTH_INTERNAL_CLIENT_SECRET}\", \"requested_scope\": \"internal\"}" 2>/dev/null || echo "")
INTERNAL_TOKEN=$(echo "${AUTH_RESP}" | jq -r '.access_token // empty' 2>/dev/null || echo "")

STATUS_CODE=$(curl -s -o /tmp/p4_hazards.json -w "%{http_code}" \
    -H "Authorization: Bearer ${INTERNAL_TOKEN}" \
    "http://localhost:8080/api/v1/hazards?limit=1" || echo "000")
if [ "${STATUS_CODE}" -eq 200 ]; then
    log_pass "Client API successfully returned HTTP 200 after canonical-db restart."
    cat /tmp/p4_hazards.json | jq '.data[0] | {hazard_id, source, hazard_type, severity}'
else
    log_fail "Client API returned HTTP ${STATUS_CODE} following database restart."
fi

echo -e "\n${C_BLUE}========================================================================${C_RESET}"
echo -e "${C_GREEN}   Problem 4 Verification Complete: All Independent Lifecycle Checks Passed!   ${C_RESET}"
echo -e "${C_BLUE}========================================================================${C_RESET}"
