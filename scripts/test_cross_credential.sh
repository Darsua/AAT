#!/usr/bin/env bash
# ==============================================================================
# Problem 3 (Criterion 1): Cross-Domain Credential Rejection
# Demonstrates that BMKG and PVMBG have fully independent trust domains:
#   - A valid BMKG credential (X-BMKG-Key) must be REJECTED (401/403) when used
#     against PVMBG.
#   - A valid PVMBG credential (Bearer token) must be REJECTED (401/403) when
#     used against BMKG.
# This directly satisfies the official M1 spec requirement:
#   "Tunjukkan kredensial yang valid untuk BMKG ditolak dengan 401/403 saat
#    dipakai mengakses PVMBG, dan sebaliknya."
# ==============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

BMKG_URL="${BMKG_URL:-http://localhost:8081}"
PVMBG_URL="${PVMBG_URL:-http://localhost:8082}"
BMKG_KEY="${BMKG_KEY:-${BMKG_API_KEY:-bmkg-secret-key-2026}}"
PVMBG_TOKEN="${PVMBG_TOKEN:-pvmbg-bearer-token-2026}"

FAILURES=0

fatal() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
    exit 1
}

command -v curl >/dev/null 2>&1 ||
    fatal "Required command 'curl' is not installed"

[[ -n "${BMKG_KEY}" ]] ||
    fatal "BMKG_KEY must be set"

[[ -n "${PVMBG_TOKEN}" ]] ||
    fatal "PVMBG_TOKEN must be set"

echo -e "${BLUE}========================================================================${NC}"
echo -e "${CYAN}   Problem 3 Demo: Cross-Domain Credential Rejection (BMKG <-> PVMBG)   ${NC}"
echo -e "${BLUE}========================================================================${NC}"

# Step 0: Health checks
echo -e "\n${BLUE}[Step 0] Checking mock-bmkg and mock-pvmbg health...${NC}"
if ! curl -s -f "${BMKG_URL}/health" >/dev/null; then
    echo -e "${RED}[ERROR] mock-bmkg is not running at ${BMKG_URL}.${NC}"
    echo -e "${YELLOW}Hint: Start services with 'make up' or 'docker compose up -d'${NC}"
    exit 1
fi
if ! curl -s -f "${PVMBG_URL}/health" >/dev/null; then
    echo -e "${RED}[ERROR] mock-pvmbg is not running at ${PVMBG_URL}.${NC}"
    echo -e "${YELLOW}Hint: Start services with 'make up' or 'docker compose up -d'${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] Both mock-bmkg and mock-pvmbg are online.${NC}"

# Step 1: Sanity check -- each credential works correctly in its OWN domain.
echo -e "\n${BLUE}[Step 1] Sanity check: credentials work correctly in their own domain...${NC}"
OWN_BMKG_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X GET "${BMKG_URL}/seismic-events" \
    -H "X-BMKG-Key: ${BMKG_KEY}")
echo -e "BMKG key against BMKG: HTTP ${OWN_BMKG_STATUS}"
if [ "${OWN_BMKG_STATUS}" -eq 200 ]; then
    echo -e "${GREEN}[OK] BMKG credential is valid against its own domain.${NC}"
else
    echo -e "${RED}[FAIL] Expected 200 from BMKG with its own credential, got ${OWN_BMKG_STATUS}.${NC}"
    FAILURES=$((FAILURES + 1))
fi

OWN_PVMBG_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X GET "${PVMBG_URL}/volcanic-reports" \
    -H "Authorization: Bearer ${PVMBG_TOKEN}")
echo -e "PVMBG token against PVMBG: HTTP ${OWN_PVMBG_STATUS}"
if [ "${OWN_PVMBG_STATUS}" -eq 200 ]; then
    echo -e "${GREEN}[OK] PVMBG credential is valid against its own domain.${NC}"
else
    echo -e "${RED}[FAIL] Expected 200 from PVMBG with its own credential, got ${OWN_PVMBG_STATUS}.${NC}"
    FAILURES=$((FAILURES + 1))
fi

# Step 2: Use the valid BMKG credential against PVMBG (expected: 401/403).
echo -e "\n${BLUE}[Step 2] Using valid BMKG credential against PVMBG (expected 401/403)...${NC}"
CROSS_BMKG_ON_PVMBG=$(curl -s -o /dev/null -w "%{http_code}" -X GET "${PVMBG_URL}/volcanic-reports" \
    -H "Authorization: Bearer ${BMKG_KEY}")
echo -e "BMKG key presented as PVMBG bearer token: HTTP ${CROSS_BMKG_ON_PVMBG}"
if [ "${CROSS_BMKG_ON_PVMBG}" -eq 401 ] || [ "${CROSS_BMKG_ON_PVMBG}" -eq 403 ]; then
    echo -e "${GREEN}[PASS] PVMBG correctly rejected a BMKG credential (HTTP ${CROSS_BMKG_ON_PVMBG}).${NC}"
else
    echo -e "${RED}[FAIL] Expected 401/403, got ${CROSS_BMKG_ON_PVMBG}. Trust domains are not isolated!${NC}"
    FAILURES=$((FAILURES + 1))
fi

# Also try it via the X-BMKG-Key header shape against PVMBG, in case PVMBG ever
# accepts that header name too (it must not).
CROSS_BMKG_HEADER_ON_PVMBG=$(curl -s -o /dev/null -w "%{http_code}" -X GET "${PVMBG_URL}/volcanic-reports" \
    -H "X-BMKG-Key: ${BMKG_KEY}")
echo -e "BMKG key sent as X-BMKG-Key header to PVMBG (no Bearer token supplied): HTTP ${CROSS_BMKG_HEADER_ON_PVMBG}"
if [ "${CROSS_BMKG_HEADER_ON_PVMBG}" -eq 401 ] || [ "${CROSS_BMKG_HEADER_ON_PVMBG}" -eq 403 ]; then
    echo -e "${GREEN}[PASS] PVMBG correctly rejected the request (HTTP ${CROSS_BMKG_HEADER_ON_PVMBG}).${NC}"
else
    echo -e "${RED}[FAIL] Expected 401/403, got ${CROSS_BMKG_HEADER_ON_PVMBG}.${NC}"
    FAILURES=$((FAILURES + 1))
fi

# Step 3: Use the valid PVMBG credential against BMKG (expected: 401/403).
echo -e "\n${BLUE}[Step 3] Using valid PVMBG credential against BMKG (expected 401/403)...${NC}"
CROSS_PVMBG_ON_BMKG=$(curl -s -o /dev/null -w "%{http_code}" -X GET "${BMKG_URL}/seismic-events" \
    -H "X-BMKG-Key: ${PVMBG_TOKEN}")
echo -e "PVMBG token presented as X-BMKG-Key header: HTTP ${CROSS_PVMBG_ON_BMKG}"
if [ "${CROSS_PVMBG_ON_BMKG}" -eq 401 ] || [ "${CROSS_PVMBG_ON_BMKG}" -eq 403 ]; then
    echo -e "${GREEN}[PASS] BMKG correctly rejected a PVMBG credential (HTTP ${CROSS_PVMBG_ON_BMKG}).${NC}"
else
    echo -e "${RED}[FAIL] Expected 401/403, got ${CROSS_PVMBG_ON_BMKG}. Trust domains are not isolated!${NC}"
    FAILURES=$((FAILURES + 1))
fi

# Step 4: No credential at all (expected: 401 on both).
echo -e "\n${BLUE}[Step 4] Sanity check: no credential at all is rejected on both mocks...${NC}"
NO_CRED_BMKG=$(curl -s -o /dev/null -w "%{http_code}" -X GET "${BMKG_URL}/seismic-events")
NO_CRED_PVMBG=$(curl -s -o /dev/null -w "%{http_code}" -X GET "${PVMBG_URL}/volcanic-reports")
echo -e "BMKG with no credential: HTTP ${NO_CRED_BMKG}"
echo -e "PVMBG with no credential: HTTP ${NO_CRED_PVMBG}"
if [ "${NO_CRED_BMKG}" -eq 401 ] || [ "${NO_CRED_BMKG}" -eq 403 ]; then
    echo -e "${GREEN}[PASS] BMKG rejects unauthenticated requests.${NC}"
else
    echo -e "${RED}[FAIL] Expected 401/403 from BMKG with no credential, got ${NO_CRED_BMKG}.${NC}"
    FAILURES=$((FAILURES + 1))
fi
if [ "${NO_CRED_PVMBG}" -eq 401 ] || [ "${NO_CRED_PVMBG}" -eq 403 ]; then
    echo -e "${GREEN}[PASS] PVMBG rejects unauthenticated requests.${NC}"
else
    echo -e "${RED}[FAIL] Expected 401/403 from PVMBG with no credential, got ${NO_CRED_PVMBG}.${NC}"
    FAILURES=$((FAILURES + 1))
fi

echo -e "\n${BLUE}========================================================================${NC}"
if [ "${FAILURES}" -eq 0 ]; then
    echo -e "${GREEN}   Problem 3 Cross-Credential Verification Complete: Trust Domains Isolated!   ${NC}"
    echo -e "${BLUE}========================================================================${NC}"
    exit 0
else
    echo -e "${RED}   Problem 3 Cross-Credential Verification FAILED: ${FAILURES} check(s) did not pass.   ${NC}"
    echo -e "${BLUE}========================================================================${NC}"
    exit 1
fi
