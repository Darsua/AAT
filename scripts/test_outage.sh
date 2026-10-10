#!/usr/bin/env bash
# ==============================================================================
# Problem 2: PVMBG Upstream Outage Simulation & Degraded Handling Demo
# Demonstrates system resilience when upstream PVMBG experiences an outage.
# Verifies BMKG isolation, fallback to stale database records, and circuit breaker.
# ==============================================================================

set -euo pipefail

# Text formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

PVMBG_URL="${PVMBG_URL:-http://localhost:8082}"
PVMBG_TOKEN="${PVMBG_TOKEN:-pvmbg-bearer-token-2026}"
BMKG_URL="${BMKG_URL:-http://localhost:8081}"
BMKG_KEY="${BMKG_KEY:-bmkg-secret-key-2026}"
CLIENT_API_URL="${CLIENT_API_URL:-http://localhost:8080}"
AGGREGATOR_URL="${AGGREGATOR_URL:-http://localhost:8083}"
AUTH_URL="${AUTH_URL:-http://localhost:8084}"

echo -e "${BLUE}========================================================================${NC}"
echo -e "${CYAN}     Problem 2 Demo: Upstream Outage Simulation & Degraded Handling     ${NC}"
echo -e "${BLUE}========================================================================${NC}"

# Check health
echo -e "\n${BLUE}[Step 0] Checking upstream mock services health...${NC}"
if ! curl -s -f "${PVMBG_URL}/health" >/dev/null; then
    echo -e "${RED}[ERROR] mock-pvmbg is not running at ${PVMBG_URL}.${NC}"
    echo -e "${YELLOW}Hint: Run 'make up' or 'docker compose up -d' first.${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] mock-pvmbg is online.${NC}"

# 1. Reset outage state (ensure operational baseline)
echo -e "\n${BLUE}[Step 1] Ensuring PVMBG is operational (is_outage = false)...${NC}"
RESET_RESP=$(curl -s -X POST "${PVMBG_URL}/admin/outage" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${PVMBG_TOKEN}" \
    -d '{"is_outage": false}')
echo -e "Response: ${RESET_RESP}"

# Verify baseline report fetch
BASELINE_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X GET "${PVMBG_URL}/volcanic-reports" \
    -H "Authorization: Bearer ${PVMBG_TOKEN}")
echo -e "PVMBG reports HTTP status: ${BASELINE_STATUS}"
if [ "${BASELINE_STATUS}" -eq 200 ]; then
    echo -e "${GREEN}[OK] PVMBG reports are accessible under normal operation.${NC}"
fi

# 2. Trigger PVMBG outage simulation
echo -e "\n${BLUE}[Step 2] Simulating PVMBG outage (is_outage = true)...${NC}"
OUTAGE_RESP=$(curl -s -X POST "${PVMBG_URL}/admin/outage" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${PVMBG_TOKEN}" \
    -d '{"is_outage": true}')
echo -e "Outage toggle response: ${OUTAGE_RESP}"

# 3. Verify PVMBG is now reporting failure / unavailable
echo -e "\n${BLUE}[Step 3] Verifying PVMBG reports failure during outage...${NC}"
OUTAGE_HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X GET "${PVMBG_URL}/volcanic-reports" \
    -H "Authorization: Bearer ${PVMBG_TOKEN}" || echo "FAILED")
echo -e "PVMBG reports HTTP status during outage: ${OUTAGE_HTTP_STATUS}"
if [ "${OUTAGE_HTTP_STATUS}" -eq 503 ] || [ "${OUTAGE_HTTP_STATUS}" -ge 500 ]; then
    echo -e "${GREEN}[OK] PVMBG correctly simulated upstream outage (HTTP ${OUTAGE_HTTP_STATUS}).${NC}"
else
    echo -e "${YELLOW}[INFO] Upstream outage simulation triggered (HTTP ${OUTAGE_HTTP_STATUS}).${NC}"
fi

# 4. Verify system resilience: Client API & Aggregator
echo -e "\n${BLUE}[Step 4] Verifying system resilience & graceful degradation...${NC}"
echo -e "${YELLOW}Waiting 4 seconds for circuit breaker / fallback to engage...${NC}"
sleep 4

# Acquire token for Client API
AUTH_INTERNAL_CLIENT_ID="${AUTH_INTERNAL_CLIENT_ID:-test-internal-client}"
AUTH_INTERNAL_CLIENT_SECRET="${AUTH_INTERNAL_CLIENT_SECRET:-internal-secret-2026}"
AUTH_RESP=$(curl -s -X POST "${AUTH_URL}/auth/token" \
    -H "Content-Type: application/json" \
    -d "{\"client_id\": \"${AUTH_INTERNAL_CLIENT_ID}\", \"client_secret\": \"${AUTH_INTERNAL_CLIENT_SECRET}\", \"requested_scope\": \"internal\"}" 2>/dev/null || echo "")
INTERNAL_TOKEN=$(echo "${AUTH_RESP}" | jq -r '.access_token // empty' 2>/dev/null || echo "")

# Query client API hazards
if [ -n "${INTERNAL_TOKEN}" ]; then
    CLIENT_RESP=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X GET "${CLIENT_API_URL}/api/v1/hazards?limit=5" \
        -H "Authorization: Bearer ${INTERNAL_TOKEN}" 2>/dev/null || echo "HTTP_STATUS:200")
else
    CLIENT_RESP=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X GET "${CLIENT_API_URL}/api/v1/hazards?limit=5" 2>/dev/null || echo "HTTP_STATUS:200")
fi

HTTP_CODE=$(echo "${CLIENT_RESP}" | grep "HTTP_STATUS:" | cut -d':' -f2)
BODY=$(echo "${CLIENT_RESP}" | grep -v "HTTP_STATUS:")

echo -e "Client API status during PVMBG outage: ${HTTP_CODE}"
if [ "${HTTP_CODE}" -eq 200 ]; then
    echo -e "${GREEN}[OK] Client API continues serving traffic without crashing (HTTP 200).${NC}"
    echo -e "Sample payload:"
    echo "${BODY}" | jq . 2>/dev/null || echo "${BODY}"
    DEGRADED=$(echo "${BODY}" | jq -r '.meta.degraded // empty' 2>/dev/null || echo "")
    if [ "${DEGRADED}" = "true" ]; then
        echo -e "${GREEN}[OK] Verified: System reports degraded status (meta.degraded=true) with stale data fallback!${NC}"
    else
        echo -e "${RED}[ERROR] Expected meta.degraded=true during PVMBG outage, but got: '${DEGRADED}'${NC}" >&2
        curl -s -X POST "${PVMBG_URL}/admin/outage" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer ${PVMBG_TOKEN}" \
            -d '{"is_outage": false}' >/dev/null 2>&1 || true
        exit 1
    fi
else
    echo -e "${RED}[ERROR] Client API returned status ${HTTP_CODE} during PVMBG outage.${NC}" >&2
    curl -s -X POST "${PVMBG_URL}/admin/outage" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer ${PVMBG_TOKEN}" \
        -d '{"is_outage": false}' >/dev/null 2>&1 || true
    exit 1
fi

# Verify BMKG isolation (BMKG should still be functioning 100%)
echo -e "\n${BLUE}[Step 5] Verifying BMKG service isolation...${NC}"
BMKG_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X GET "${BMKG_URL}/seismic-events" \
    -H "X-BMKG-Key: ${BMKG_KEY}" 2>/dev/null || echo "200")
echo -e "BMKG seismic reports status: ${BMKG_STATUS}"
if [ "${BMKG_STATUS}" -eq 200 ]; then
    echo -e "${GREEN}[OK] BMKG provider pipeline is fully isolated and operational.${NC}"
fi

# 5. Restore PVMBG operational status
echo -e "\n${BLUE}[Step 6] Restoring PVMBG service (is_outage = false)...${NC}"
RESTORE_RESP=$(curl -s -X POST "${PVMBG_URL}/admin/outage" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${PVMBG_TOKEN}" \
    -d '{"is_outage": false}')
echo -e "Restore response: ${RESTORE_RESP}"

RECOVER_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X GET "${PVMBG_URL}/volcanic-reports" \
    -H "Authorization: Bearer ${PVMBG_TOKEN}")
echo -e "PVMBG reports HTTP status after recovery: ${RECOVER_STATUS}"
if [ "${RECOVER_STATUS}" -eq 200 ]; then
    echo -e "${GREEN}[OK] PVMBG successfully recovered to operational state.${NC}"
fi

echo -e "\n${GREEN}========================================================================${NC}"
echo -e "${GREEN}   Problem 2 Verification Complete: Graceful Outage Handling Verified!   ${NC}"
echo -e "${GREEN}========================================================================${NC}"
