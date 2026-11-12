# Problem 5: Fan-out and consumer recovery — historical M1 evidence

**Date context: 2026-10-08**, derived from epoch run ID `1791471484` in the log and the script's use of `date +%s`, not from synthetic payload timestamps. This evidence is not current validation or an M2 guarantee. No tests were rerun during this documentation cleanup.

## Recorded execution and results

[`consumer_recovery.log`](consumer_recovery.log), from [`test_consumer_recovery.sh`](../../../../scripts/test_consumer_recovery.sh), records **11 passed, 0 failed**:

- Redpanda responded, and topic `bnpb.hazard-events` had **3 partitions**.
- The same event was recorded by two independent groups: `bnpb-dashboard-group` and `bnpb-notifier-group`.
- While Dashboard was stopped, Notifier processed downtime events, and Dashboard's consumer-group lag reached **5**.
- After Dashboard restarted, it processed the outstanding events, and lag fell to **0**.
- A third group, `bnpb-evaluator-audit-group`, received events without changes to or restarts of the publisher/Aggregator.

## Evidence limitations

The script publishes test events **directly to the broker** using `rpk`; this establishes the observed broker/consumer path, not end-to-end ingestion or outbox relay. Recovery depends on message retention and committed offsets remaining available. The test does not establish node-loss durability, exactly-once effects, or processing of hazard revisions: hazard-ID deduplication and consumer counters are process-local. See [ADR 002](../../../adr/002-message-broker-redpanda.md) and [ADR 005](../../../adr/005-stateless-architecture-and-m2-readiness.md).
