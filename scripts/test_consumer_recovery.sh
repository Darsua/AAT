#!/usr/bin/env bash
# ==============================================================================
# Problem 5: Pub-Sub Fan-Out, Consumer Downtime Recovery & Dynamic 3rd Consumer
# Demonstrates:
#   1. Dashboard and Notifier receive the same event through separate groups.
#   2. One consumer can stop while the other continues processing.
#   3. The stopped consumer resumes from its committed offset after restart.
#   4. A 3rd consumer group can join without changing the Aggregator.
# ==============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

TOPIC="${TOPIC:-bnpb.hazard-events}"
EXPECTED_PARTITIONS="${EXPECTED_PARTITIONS:-3}"
CONSUMER_1_SERVICE="${CONSUMER_1_SERVICE:-dashboard-consumer}"
CONSUMER_1_GROUP="${CONSUMER_1_GROUP:-bnpb-dashboard-group}"
CONSUMER_2_SERVICE="${CONSUMER_2_SERVICE:-notifier-consumer}"
CONSUMER_2_GROUP="${CONSUMER_2_GROUP:-bnpb-notifier-group}"
AUDIT_GROUP="${AUDIT_GROUP:-bnpb-evaluator-audit-group}"
DOCKER_COMPOSE="${DOCKER_COMPOSE:-}"
DOCKER="${DOCKER:-}"

PASSES=0
FAILURES=0
AUDIT_CONTAINER=""
RUN_ID="$(date +%s)-$$"

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

# Kafka delivery is asynchronous, so give a consumer a short time to write its log.
wait_for_compose_log() {
    local service="$1"
    local event_id="$2"

    for _ in {1..15}; do
        if ${DOCKER_COMPOSE} logs --no-color "${service}" 2>/dev/null | grep -Fq "${event_id}"; then
            return 0
        fi
        sleep 1
    done
    return 1
}

cleanup() {
    if [[ -n "${AUDIT_CONTAINER}" ]]; then
        ${DOCKER} rm -f "${AUDIT_CONTAINER}" >/dev/null 2>&1 || true
    fi
    ${DOCKER_COMPOSE} start "${CONSUMER_1_SERVICE}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || fatal "Required command 'docker' is not installed"

if [[ -z "${DOCKER_COMPOSE}" ]]; then
    if docker info >/dev/null 2>&1; then
        DOCKER_COMPOSE="docker compose"
        DOCKER="${DOCKER:-docker}"
    else
        DOCKER_COMPOSE="sudo docker compose"
        DOCKER="${DOCKER:-sudo docker}"
    fi
fi
DOCKER="${DOCKER:-docker}"

echo -e "${BLUE}========================================================================${NC}"
echo -e "${CYAN}   Problem 5 Demo: Event Streaming, Downtime Recovery & 3rd Consumer    ${NC}"
echo -e "${BLUE}========================================================================${NC}"

echo -e "\n${BLUE}[Step 0] Checking Redpanda and the configured topic...${NC}"
if ! ${DOCKER_COMPOSE} exec -T redpanda rpk cluster info >/dev/null 2>&1; then
    fatal "Redpanda is unavailable. Start the platform with 'docker compose up -d'."
fi
pass "Redpanda is responsive."

PARTITION_OUTPUT="$(${DOCKER_COMPOSE} exec -T redpanda rpk topic describe "${TOPIC}" --print-partitions 2>/dev/null)" ||
    fatal "Topic '${TOPIC}' does not exist"
echo "${PARTITION_OUTPUT}"
ACTUAL_PARTITIONS="$(awk 'NR > 1 && $1 ~ /^[0-9]+$/ { count++ } END { print count + 0 }' <<<"${PARTITION_OUTPUT}")"
if [[ "${ACTUAL_PARTITIONS}" == "${EXPECTED_PARTITIONS}" ]]; then
    pass "Topic '${TOPIC}' has ${EXPECTED_PARTITIONS} partitions."
else
    fail "Topic '${TOPIC}' has ${ACTUAL_PARTITIONS} partitions; expected ${EXPECTED_PARTITIONS}."
fi

echo -e "\n${BLUE}[Step 1] Starting the Dashboard and Notifier consumers...${NC}"
if ! ${DOCKER_COMPOSE} config --services | grep -Fxq "${CONSUMER_1_SERVICE}" ||
    ! ${DOCKER_COMPOSE} config --services | grep -Fxq "${CONSUMER_2_SERVICE}"; then
    fatal "Dashboard or Notifier is not registered in docker-compose.yml"
fi
${DOCKER_COMPOSE} up -d "${CONSUMER_1_SERVICE}" "${CONSUMER_2_SERVICE}" >/dev/null
sleep 2

RUNNING_SERVICES="$(${DOCKER_COMPOSE} ps --status running --services)"
if grep -Fxq "${CONSUMER_1_SERVICE}" <<<"${RUNNING_SERVICES}" &&
    grep -Fxq "${CONSUMER_2_SERVICE}" <<<"${RUNNING_SERVICES}"; then
    pass "Dashboard and Notifier containers are running."
else
    fatal "Dashboard or Notifier did not start"
fi

echo -e "\n${BLUE}[Step 2] Publishing one event for both independent consumer groups...${NC}"
FANOUT_EVENT_ID="c08-fanout-${RUN_ID}"
FANOUT_EVENT="{\"hazard_id\":\"${FANOUT_EVENT_ID}\",\"source\":\"BMKG\",\"source_ref_id\":\"${FANOUT_EVENT_ID}\",\"hazard_type\":\"SEISMIC\",\"severity\":\"SIAGA\",\"area_name\":\"C-08 Fan-Out Test\",\"occurred_at\":\"2026-10-06T00:00:00Z\",\"ingested_at\":\"2026-10-06T00:00:01Z\",\"attributes\":{\"magnitude\":6.7}}"
printf '%s\n' "${FANOUT_EVENT}" |
    ${DOCKER_COMPOSE} exec -T redpanda rpk topic produce "${TOPIC}" -k "${FANOUT_EVENT_ID}" >/dev/null

if wait_for_compose_log "${CONSUMER_1_SERVICE}" "${FANOUT_EVENT_ID}"; then
    pass "Dashboard received event '${FANOUT_EVENT_ID}'."
else
    fail "Dashboard did not log event '${FANOUT_EVENT_ID}'."
fi
if wait_for_compose_log "${CONSUMER_2_SERVICE}" "${FANOUT_EVENT_ID}"; then
    pass "Notifier received the same event '${FANOUT_EVENT_ID}'."
else
    fail "Notifier did not log event '${FANOUT_EVENT_ID}'."
fi

echo -e "\n${BLUE}[Step 3] Stopping Dashboard and publishing an event during downtime...${NC}"
${DOCKER_COMPOSE} stop "${CONSUMER_1_SERVICE}" >/dev/null
RECOVERY_EVENT_ID="c08-recovery-${RUN_ID}"
RECOVERY_EVENT="{\"hazard_id\":\"${RECOVERY_EVENT_ID}\",\"source\":\"PVMBG\",\"source_ref_id\":\"${RECOVERY_EVENT_ID}\",\"hazard_type\":\"VOLCANIC\",\"severity\":\"AWAS\",\"area_name\":\"C-08 Recovery Test\",\"occurred_at\":\"2026-10-06T00:01:00Z\",\"ingested_at\":\"2026-10-06T00:01:01Z\",\"attributes\":{}}"
printf '%s\n' "${RECOVERY_EVENT}" |
    ${DOCKER_COMPOSE} exec -T redpanda rpk topic produce "${TOPIC}" -k "${RECOVERY_EVENT_ID}" >/dev/null

if wait_for_compose_log "${CONSUMER_2_SERVICE}" "${RECOVERY_EVENT_ID}"; then
    pass "Notifier continued processing while Dashboard was stopped."
else
    fail "Notifier did not process the downtime event."
fi

if ${DOCKER_COMPOSE} logs --no-color "${CONSUMER_1_SERVICE}" 2>/dev/null | grep -Fq "${RECOVERY_EVENT_ID}"; then
    fail "Dashboard processed the recovery event before it was restarted."
else
    pass "The recovery event remained pending for the stopped Dashboard group."
fi

LAG_OUTPUT="$(${DOCKER_COMPOSE} exec -T redpanda rpk group describe "${CONSUMER_1_GROUP}" --print-summary 2>/dev/null || true)"
echo "${LAG_OUTPUT}"
DOWNTIME_LAG="$(awk '$1 == "TOTAL-LAG" { print $2 }' <<<"${LAG_OUTPUT}")"
if [[ "${DOWNTIME_LAG}" =~ ^[0-9]+$ ]] && ((DOWNTIME_LAG >= 1)); then
    pass "Dashboard group accumulated lag while offline."
else
    fail "Expected Dashboard lag of at least 1, got '${DOWNTIME_LAG:-unknown}'."
fi

echo -e "\n${BLUE}[Step 4] Restarting Dashboard and verifying backlog recovery...${NC}"
${DOCKER_COMPOSE} start "${CONSUMER_1_SERVICE}" >/dev/null
if wait_for_compose_log "${CONSUMER_1_SERVICE}" "${RECOVERY_EVENT_ID}"; then
    pass "Dashboard resumed from its committed offset and processed the missed event."
else
    fail "Dashboard did not process the missed event after restart."
fi

RECOVERED_LAG=""
for _ in {1..10}; do
    LAG_OUTPUT="$(${DOCKER_COMPOSE} exec -T redpanda rpk group describe "${CONSUMER_1_GROUP}" --print-summary 2>/dev/null || true)"
    RECOVERED_LAG="$(awk '$1 == "TOTAL-LAG" { print $2 }' <<<"${LAG_OUTPUT}")"
    [[ "${RECOVERED_LAG}" == "0" ]] && break
    sleep 1
done
echo "${LAG_OUTPUT}"
if [[ "${RECOVERED_LAG}" == "0" ]]; then
    pass "Dashboard consumer-group lag returned to zero."
else
    fail "Dashboard lag did not return to zero; current value is '${RECOVERED_LAG:-unknown}'."
fi

echo -e "\n${BLUE}[Step 5] Adding a dynamic 3rd consumer group...${NC}"
AUDIT_CONTAINER="bnpb-c08-audit-${RUN_ID}"
${DOCKER_COMPOSE} run --rm -d --no-deps \
    --name "${AUDIT_CONTAINER}" \
    -e REDPANDA_BROKERS=redpanda:9092 \
    -e TOPIC="${TOPIC}" \
    -e CONSUMER_GROUP="${AUDIT_GROUP}-${RUN_ID}" \
    "${CONSUMER_1_SERVICE}" >/dev/null
sleep 2

AUDIT_EVENT_ID="c08-audit-${RUN_ID}"
AUDIT_EVENT="{\"hazard_id\":\"${AUDIT_EVENT_ID}\",\"source\":\"BMKG\",\"source_ref_id\":\"${AUDIT_EVENT_ID}\",\"hazard_type\":\"SEISMIC\",\"severity\":\"WASPADA\",\"area_name\":\"C-08 Audit Test\",\"occurred_at\":\"2026-10-06T00:02:00Z\",\"ingested_at\":\"2026-10-06T00:02:01Z\",\"attributes\":{\"magnitude\":5.8}}"
printf '%s\n' "${AUDIT_EVENT}" |
    ${DOCKER_COMPOSE} exec -T redpanda rpk topic produce "${TOPIC}" -k "${AUDIT_EVENT_ID}" >/dev/null

AUDIT_RECEIVED=0
for _ in {1..20}; do
    if ${DOCKER} logs "${AUDIT_CONTAINER}" 2>&1 | grep -Fq "${AUDIT_EVENT_ID}"; then
        AUDIT_RECEIVED=1
        break
    fi
    sleep 1
done
if [[ "${AUDIT_RECEIVED}" == "1" ]]; then
    pass "A new 3rd consumer group received events without publisher changes."
else
    fail "The dynamic 3rd consumer did not receive '${AUDIT_EVENT_ID}'."
fi

echo -e "\n${BLUE}Results:${NC} ${PASSES} passed, ${FAILURES} failed"
if ((FAILURES > 0)); then
    exit 1
fi

echo -e "\n${GREEN}========================================================================${NC}"
echo -e "${GREEN}   Problem 5 Verification Complete: Fan-Out & Recovery Verified!        ${NC}"
echo -e "${GREEN}========================================================================${NC}"
