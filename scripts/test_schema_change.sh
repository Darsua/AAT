#!/usr/bin/env bash
# ==============================================================================
# Problem 1: Schema Change & Dynamic Field Evolution Demo
# Demonstrates upstream schema changes (PVMBG confidence_level toggle)
# and verifies ingestion into canonical JSONB attributes without schema breaking.
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
PVMBG_TOKEN="${PVMBG_TOKEN:-}"
CLIENT_API_URL="${CLIENT_API_URL:-http://localhost:8080}"
AUTH_URL="${AUTH_URL:-http://localhost:8084}"
AUTH_INTERNAL_CLIENT_ID="${AUTH_INTERNAL_CLIENT_ID:-test-internal-client}"
AUTH_INTERNAL_CLIENT_SECRET="${AUTH_INTERNAL_CLIENT_SECRET:-}"

command -v curl >/dev/null 2>&1 || { echo -e "${RED}[ERROR] curl is required${NC}" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo -e "${RED}[ERROR] jq is required${NC}" >&2; exit 1; }
[ -n "${PVMBG_TOKEN}" ] || { echo -e "${RED}[ERROR] PVMBG_TOKEN must be set${NC}" >&2; exit 1; }
[ -n "${AUTH_INTERNAL_CLIENT_SECRET}" ] || { echo -e "${RED}[ERROR] AUTH_INTERNAL_CLIENT_SECRET must be set${NC}" >&2; exit 1; }

SCHEMA_MODIFIED=0
restore_schema() {
    if [ "${SCHEMA_MODIFIED}" -eq 1 ]; then
        curl -s -X POST "${PVMBG_URL}/admin/schema-version" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer ${PVMBG_TOKEN}" \
            -d '{"include_confidence_level": false}' >/dev/null 2>&1 || true
    fi
}
trap restore_schema EXIT

echo -e "${BLUE}========================================================================${NC}"
echo -e "${CYAN}   Problem 1 Demo: Dynamic Schema Evolution (PVMBG Confidence Level)     ${NC}"
echo -e "${BLUE}========================================================================${NC}"

# Check PVMBG health
echo -e "\n${BLUE}[Step 0] Checking upstream mock-pvmbg service health...${NC}"
if ! curl -s -f "${PVMBG_URL}/health" >/dev/null; then
    echo -e "${RED}[ERROR] mock-pvmbg is not running or reachable at ${PVMBG_URL}.${NC}"
    echo -e "${YELLOW}Hint: Start services with 'make up' or 'docker compose up -d'${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] mock-pvmbg is healthy.${NC}"

# 1. Reset schema version to false (baseline without confidence_level)
echo -e "\n${BLUE}[Step 1] Setting baseline schema: include_confidence_level = false...${NC}"
RESET_RESP=$(curl -s -X POST "${PVMBG_URL}/admin/schema-version" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${PVMBG_TOKEN}" \
    -d '{"include_confidence_level": false}')
echo -e "Response from /admin/schema-version: ${RESET_RESP}"

# Verify baseline reports
echo -e "\n${BLUE}[Step 2] Querying PVMBG reports with baseline schema...${NC}"
BASELINE_REPORTS=$(curl -s -X GET "${PVMBG_URL}/volcanic-reports" \
    -H "Authorization: Bearer ${PVMBG_TOKEN}")
echo -e "Sample baseline report output:"
echo "${BASELINE_REPORTS}" | jq . 2>/dev/null || echo "${BASELINE_REPORTS}"

# Check that confidence_level is not present
if echo "${BASELINE_REPORTS}" | grep -q '"confidence_level"'; then
    echo -e "${YELLOW}[WARN] confidence_level unexpectedly found in baseline reports.${NC}"
else
    echo -e "${GREEN}[OK] Verified: baseline reports do NOT contain confidence_level.${NC}"
fi

# 2. Trigger schema change: include_confidence_level = true
echo -e "\n${BLUE}[Step 3] Triggering schema change: include_confidence_level = true...${NC}"
SCHEMA_MODIFIED=1
UPDATE_RESP=$(curl -s -f -X POST "${PVMBG_URL}/admin/schema-version" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${PVMBG_TOKEN}" \
    -d '{"include_confidence_level": true}')
echo -e "Response from /admin/schema-version: ${UPDATE_RESP}"

# 3. Verify reports now include confidence_level
echo -e "\n${BLUE}[Step 4] Querying PVMBG reports with updated schema...${NC}"
EVOLVED_REPORTS=$(curl -s -f -X GET "${PVMBG_URL}/volcanic-reports" \
    -H "Authorization: Bearer ${PVMBG_TOKEN}")
echo -e "Sample evolved report output:"
echo "${EVOLVED_REPORTS}" | jq . 2>/dev/null || echo "${EVOLVED_REPORTS}"

if echo "${EVOLVED_REPORTS}" | grep -q '"confidence_level"'; then
    echo -e "${GREEN}[OK] Verified: Upstream reports successfully include dynamic 'confidence_level'.${NC}"
else
    echo -e "${RED}[ERROR] Expected dynamic field 'confidence_level' in evolved PVMBG reports, but it was missing.${NC}" >&2
    exit 1
fi

# 4. Ingestion verification in Aggregator / Client API
echo -e "\n${BLUE}[Step 5] Verifying canonical ingestion tolerance in Client API / Aggregator...${NC}"
echo -e "${YELLOW}Waiting 4 seconds for aggregator polling cycle...${NC}"
sleep 4

# Acquire token for Client API
AUTH_PAYLOAD="$(jq -cn \
    --arg client_id "${AUTH_INTERNAL_CLIENT_ID}" \
    --arg client_secret "${AUTH_INTERNAL_CLIENT_SECRET}" \
    '{client_id: $client_id, client_secret: $client_secret, requested_scope: "internal"}')"
AUTH_RESP=$(curl -s -X POST "${AUTH_URL}/auth/token" \
    -H "Content-Type: application/json" \
    -d "${AUTH_PAYLOAD}" 2>/dev/null || echo "")
INTERNAL_TOKEN=$(echo "${AUTH_RESP}" | jq -r '.access_token // empty' 2>/dev/null || echo "")
if [ -z "${INTERNAL_TOKEN}" ]; then
    echo -e "${RED}[ERROR] Failed to acquire internal access token from ${AUTH_URL}/auth/token.${NC}" >&2
    exit 1
fi

# Check client API hazards
HAZARDS_RESP=$(curl -s -f -X GET "${CLIENT_API_URL}/api/v1/hazards?type=VOLCANIC&fields=all&limit=5" \
    -H "Authorization: Bearer ${INTERNAL_TOKEN}" 2>/dev/null || echo "")

if [ -n "${HAZARDS_RESP}" ]; then
    echo -e "Canonical hazards response:"
    echo "${HAZARDS_RESP}" | jq . 2>/dev/null || echo "${HAZARDS_RESP}"
    if echo "${HAZARDS_RESP}" | grep -q '"confidence_level"'; then
        echo -e "${GREEN}[OK] Verified: Canonical hazards successfully contain dynamic 'confidence_level' in attributes!${NC}"
        echo -e "${GREEN}[OK] Aggregator digested dynamic schema without schema rupture or crash.${NC}"
    else
        echo -e "${RED}[ERROR] Dynamic field 'confidence_level' not found in canonical hazards response!${NC}" >&2
        exit 1
    fi
else
    echo -e "${RED}[ERROR] Client API hazard endpoint returned empty response!${NC}" >&2
    exit 1
fi

echo -e "\n${GREEN}========================================================================${NC}"
echo -e "${GREEN}   Problem 1 Verification Complete: Schema Evolution Handled Cleanly!    ${NC}"
echo -e "${GREEN}========================================================================${NC}"
