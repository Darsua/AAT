#!/usr/bin/env bash
# ==============================================================================
# Problem 3: Role-Based Access Control (RBAC) & Scope Verification Demo
# Demonstrates:
#   1. Token acquisition for Media, Field Responder, and Internal Admin roles.
#   2. Media role accessing summary fields (HTTP 200 OK).
#   3. Media role attempting to access raw telemetry fields (HTTP 403 Forbidden).
#   4. Field responder role accessing raw coordinates/telemetry (HTTP 200 OK).
#   5. Internal role accessing full hazard datasets (HTTP 200 OK).
#   6. Refresh token flow (issuance of rotated access token).
# ==============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

AUTH_URL="${AUTH_URL:-http://localhost:8084}"
CLIENT_API_URL="${CLIENT_API_URL:-http://localhost:8080}"

MEDIA_CLIENT_ID="${AUTH_MEDIA_CLIENT_ID:-test-media-client}"
FIELD_CLIENT_ID="${AUTH_FIELD_CLIENT_ID:-test-field-client}"
INTERNAL_CLIENT_ID="${AUTH_INTERNAL_CLIENT_ID:-test-internal-client}"

# Secrets default to standard development credentials matching docker-compose.yml if not set
MEDIA_CLIENT_SECRET="${AUTH_MEDIA_CLIENT_SECRET:-media-secret-2026}"
FIELD_CLIENT_SECRET="${AUTH_FIELD_CLIENT_SECRET:-field-secret-2026}"
INTERNAL_CLIENT_SECRET="${AUTH_INTERNAL_CLIENT_SECRET:-internal-secret-2026}"

PASSES=0
FAILURES=0
HTTP_STATUS=""
HTTP_BODY=""
ACCESS_TOKEN=""
REFRESH_TOKEN=""
EXPIRES_IN=""

TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TEMP_DIR}"' EXIT
RESPONSE_FILE="${TEMP_DIR}/response.json"

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

# Store the real response body and status separately. A connection failure stops
# the test instead of being replaced with a fabricated successful response.
request_json() {
    local method="$1"
    local url="$2"
    local payload="${3:-}"
    local bearer_token="${4:-}"
    local correlation_id="${5:-}"
    local -a arguments=(
        --silent
        --show-error
        --output "${RESPONSE_FILE}"
        --write-out "%{http_code}"
        --request "${method}"
        --header "Accept: application/json"
    )

    if [[ -n "${payload}" ]]; then
        arguments+=(--header "Content-Type: application/json" --data "${payload}")
    fi
    if [[ -n "${bearer_token}" ]]; then
        arguments+=(--header "Authorization: Bearer ${bearer_token}")
    fi
    if [[ -n "${correlation_id}" ]]; then
        arguments+=(--header "X-Correlation-ID: ${correlation_id}")
    fi

    if ! HTTP_STATUS="$(curl "${arguments[@]}" "${url}")"; then
        fatal "${method} ${url}: connection failed"
    fi
    HTTP_BODY="$(<"${RESPONSE_FILE}")"
}

expect_status() {
    local expected="$1"
    local scenario="$2"

    echo "HTTP Status Code: ${HTTP_STATUS}"
    if [[ "${HTTP_STATUS}" == "${expected}" ]]; then
        pass "${scenario}"
    else
        fail "${scenario}: expected HTTP ${expected}, got ${HTTP_STATUS}; body=${HTTP_BODY}"
    fi
}

# Acquire a real token and validate the response contract. The arguments come
# from each explicit role step below so the test flow remains easy to follow.
get_token() {
    local role="$1"
    local client_id="$2"
    local client_secret="$3"
    local expected_scope="$4"
    local payload
    local actual_scope

    payload="$(jq -cn \
        --arg client_id "${client_id}" \
        --arg client_secret "${client_secret}" \
        --arg requested_scope "${role}" \
        '{client_id: $client_id, client_secret: $client_secret, requested_scope: $requested_scope}')"

    request_json POST "${AUTH_URL}/auth/token" "${payload}"
    if [[ "${HTTP_STATUS}" != "200" ]]; then
        fatal "${role} token issuance returned HTTP ${HTTP_STATUS}; body=${HTTP_BODY}"
    fi

    if ! ACCESS_TOKEN="$(jq -er '.access_token' <<<"${HTTP_BODY}")" ||
        ! REFRESH_TOKEN="$(jq -er '.refresh_token' <<<"${HTTP_BODY}")" ||
        ! EXPIRES_IN="$(jq -er '.expires_in | select(type == "number" and . > 0)' <<<"${HTTP_BODY}")" ||
        ! actual_scope="$(jq -er '.scope' <<<"${HTTP_BODY}")"; then
        fatal "${role} token response is missing a required field"
    fi

    if [[ "${actual_scope}" != "${expected_scope}" ]]; then
        fatal "${role} token contains scope '${actual_scope}', expected '${expected_scope}'"
    fi
    pass "${role} token issued with scope '${expected_scope}'"
}

command -v curl >/dev/null 2>&1 || fatal "Required command 'curl' is not installed"
command -v jq >/dev/null 2>&1 || fatal "Required command 'jq' is not installed"

[[ -n "${MEDIA_CLIENT_SECRET}" ]] || fatal "AUTH_MEDIA_CLIENT_SECRET must be set"
[[ -n "${FIELD_CLIENT_SECRET}" ]] || fatal "AUTH_FIELD_CLIENT_SECRET must be set"
[[ -n "${INTERNAL_CLIENT_SECRET}" ]] || fatal "AUTH_INTERNAL_CLIENT_SECRET must be set"

echo -e "${BLUE}========================================================================${NC}"
echo -e "${CYAN}     Problem 3 Demo: Auth Scopes, RBAC Projections & Refresh Flow       ${NC}"
echo -e "${BLUE}========================================================================${NC}"

echo -e "\n${BLUE}[Step 0] Checking Auth Service & Client API health...${NC}"
curl --silent --show-error --fail "${AUTH_URL}/health" >/dev/null ||
    fatal "Auth service is unavailable at ${AUTH_URL}"
curl --silent --show-error --fail "${CLIENT_API_URL}/health" >/dev/null ||
    fatal "Client API is unavailable at ${CLIENT_API_URL}"
PASSES=$((PASSES + 1))
echo -e "${GREEN}[OK] Auth service and Client API are online.${NC}"

echo -e "\n${BLUE}[Security check] Confirming an invalid client secret is rejected...${NC}"
INVALID_SECRET_PAYLOAD="$(jq -cn \
    --arg client_id "${MEDIA_CLIENT_ID}" \
    --arg client_secret "invalid-c07-secret" \
    '{client_id: $client_id, client_secret: $client_secret, requested_scope: "media"}')"
request_json POST "${AUTH_URL}/auth/token" "${INVALID_SECRET_PAYLOAD}"
expect_status 401 "Auth rejected an invalid client secret (HTTP 401)."

echo -e "\n${BLUE}[Step 1] Requesting token for Media role (scope: media)...${NC}"
get_token media "${MEDIA_CLIENT_ID}" "${MEDIA_CLIENT_SECRET}" "hazards:read:summary"
MEDIA_ACCESS_TOKEN="${ACCESS_TOKEN}"

echo -e "\n${BLUE}[Step 2] Media client requests summary fields (fields=summary)...${NC}"
request_json GET "${CLIENT_API_URL}/api/v1/hazards?fields=summary&limit=1" "" "${MEDIA_ACCESS_TOKEN}" "c04-media-summary"
expect_status 200 "Media client successfully retrieved summary alerts (HTTP 200)."

if [[ "${HTTP_STATUS}" == "200" ]]; then
    echo "Summary Payload Preview:"
    jq . <<<"${HTTP_BODY}" || echo "${HTTP_BODY}"
    if ! jq -e 'type == "object" and (.data | type == "array")' >/dev/null <<<"${HTTP_BODY}"; then
        fail "Media summary response is not an object containing a data array"
    elif jq -e '.data[]? | select(has("source_ref_id") or has("latitude") or has("longitude") or has("attributes"))' \
        >/dev/null <<<"${HTTP_BODY}"; then
        fail "Media summary response leaked raw fields"
    else
        pass "Media summary response does not expose raw fields."
    fi
fi

echo -e "\n${BLUE}[Step 3] Media client attempts to access raw fields (fields=all)...${NC}"
request_json GET "${CLIENT_API_URL}/api/v1/hazards?fields=all&limit=1" "" "${MEDIA_ACCESS_TOKEN}" "c04-media-all"
expect_status 403 "Scope enforced! Media client strictly prohibited from raw telemetry (HTTP 403 Forbidden)."
if [[ "${HTTP_STATUS}" == "403" ]]; then
    echo "Rejection Response:"
    jq . <<<"${HTTP_BODY}" || echo "${HTTP_BODY}"
fi

echo -e "\n${BLUE}[Step 4] Requesting token for Field Responder role (scope: field)...${NC}"
get_token field "${FIELD_CLIENT_ID}" "${FIELD_CLIENT_SECRET}" "hazards:read:all"
FIELD_ACCESS_TOKEN="${ACCESS_TOKEN}"
FIELD_REFRESH_TOKEN="${REFRESH_TOKEN}"
FIELD_EXPIRES_IN="${EXPIRES_IN}"
echo "Field Responder requests raw telemetry (fields=all)..."
request_json GET "${CLIENT_API_URL}/api/v1/hazards?fields=all&limit=1" "" "${FIELD_ACCESS_TOKEN}" "c04-field-all"
expect_status 200 "Field Responder successfully accessed raw coordinates and sensor telemetry (HTTP 200)."

echo -e "\n${BLUE}[Step 5] Requesting token for Internal Operations Center (scope: internal)...${NC}"
get_token internal "${INTERNAL_CLIENT_ID}" "${INTERNAL_CLIENT_SECRET}" "hazards:read:all"
INTERNAL_ACCESS_TOKEN="${ACCESS_TOKEN}"
request_json GET "${CLIENT_API_URL}/api/v1/hazards?fields=all&limit=1" "" "${INTERNAL_ACCESS_TOKEN}" "c04-internal-all"
expect_status 200 "Internal Ops Center granted unrestricted access (HTTP 200)."

WAIT_SECONDS=$((FIELD_EXPIRES_IN + 2))
echo -e "\n${BLUE}[Step 6] Waiting ${WAIT_SECONDS}s for the Field access token to expire naturally (TTL=${FIELD_EXPIRES_IN}s)...${NC}"
echo -e "${YELLOW}(Set SKIP_TTL_WAIT=1 to skip this real-time wait during quick iteration; the${NC}"
echo -e "${YELLOW} full wait is required for an accurate P3 criterion-3 demo/evidence capture.)${NC}"
if [[ "${SKIP_TTL_WAIT:-0}" != "1" ]]; then
    sleep "${WAIT_SECONDS}"

    echo -e "\n${BLUE}[Step 7] Confirming the now-expired Field access token is rejected...${NC}"
    request_json GET "${CLIENT_API_URL}/api/v1/hazards?fields=all&limit=1" "" "${FIELD_ACCESS_TOKEN}" "c04-field-expired"
    expect_status 401 "Expired Field access token correctly rejected with HTTP 401."
else
    echo -e "${YELLOW}[SKIP] Natural access-token expiration check.${NC}"
fi

echo -e "\n${BLUE}[Step 8] Refreshing the Field session without manual re-login...${NC}"
REFRESH_PAYLOAD="$(jq -cn --arg refresh_token "${FIELD_REFRESH_TOKEN}" '{refresh_token: $refresh_token}')"
request_json POST "${AUTH_URL}/auth/refresh" "${REFRESH_PAYLOAD}"
expect_status 200 "Successfully refreshed the Field session without manual re-login"

NEW_ACCESS_TOKEN=""
NEW_REFRESH_TOKEN=""
if [[ "${HTTP_STATUS}" == "200" ]]; then
    if ! NEW_ACCESS_TOKEN="$(jq -er '.access_token' <<<"${HTTP_BODY}")" ||
        ! NEW_REFRESH_TOKEN="$(jq -er '.refresh_token' <<<"${HTTP_BODY}")"; then
        fail "Refresh response is missing access_token or refresh_token"
    elif [[ "${NEW_REFRESH_TOKEN}" == "${FIELD_REFRESH_TOKEN}" ]]; then
        fail "Refresh token was not rotated"
    else
        pass "Successfully refreshed and received a new ${FIELD_EXPIRES_IN}s access token and rotated refresh token."
    fi
fi

if [[ -n "${NEW_ACCESS_TOKEN}" ]]; then
    request_json GET "${CLIENT_API_URL}/api/v1/hazards?fields=all&limit=1" "" "${NEW_ACCESS_TOKEN}" "c04-field-refreshed"
    echo "API query with refreshed token HTTP status: ${HTTP_STATUS}"
    if [[ "${HTTP_STATUS}" == "200" ]]; then
        pass "Refreshed token validated successfully against Client API."
    else
        fail "Refreshed token was rejected by Client API; body=${HTTP_BODY}"
    fi
else
    fail "Refreshed access token is unavailable, so Client API verification cannot run"
fi

if [[ "${SKIP_TTL_WAIT:-0}" != "1" ]]; then
    echo -e "\n${BLUE}[Step 9] Confirming the old access token remains rejected after refresh...${NC}"
    request_json GET "${CLIENT_API_URL}/api/v1/hazards?fields=all&limit=1" "" "${FIELD_ACCESS_TOKEN}" "c04-field-old-access"
    expect_status 401 "Old access token remains rejected after refresh (HTTP 401)."
fi

echo -e "\n${BLUE}[Step 10] Confirming rotated refresh token: reusing the old refresh token is rejected...${NC}"
request_json POST "${AUTH_URL}/auth/refresh" "${REFRESH_PAYLOAD}"
expect_status 401 "Reused (rotated-out) refresh token correctly rejected."

echo -e "\n${BLUE}[Additional check] Confirming replay detection revoked the remaining Field session...${NC}"
if [[ -n "${NEW_REFRESH_TOKEN}" ]]; then
    NEW_REFRESH_PAYLOAD="$(jq -cn --arg refresh_token "${NEW_REFRESH_TOKEN}" '{refresh_token: $refresh_token}')"
    request_json POST "${AUTH_URL}/auth/refresh" "${NEW_REFRESH_PAYLOAD}"
    expect_status 401 "Refresh-token replay revokes the remaining session tokens"
else
    fail "Rotated refresh token is unavailable, so replay revocation cannot be verified"
fi

echo -e "\n${BLUE}Results:${NC} ${PASSES} passed, ${FAILURES} failed"
if ((FAILURES > 0)); then
    exit 1
fi

echo -e "\n${GREEN}========================================================================${NC}"
echo -e "${GREEN}   Problem 3 Verification Complete: RBAC & Scope Enforcement Verified!   ${NC}"
echo -e "${GREEN}========================================================================${NC}"
