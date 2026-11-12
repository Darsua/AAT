# Historical ADR measurement evidence

These **2026-10-10, single-sample** observations provide historical context for [ADR 001](../../../adr/001-service-language-all-go.md), [ADR 002](../../../adr/002-message-broker-redpanda.md), and [ADR 003](../../../adr/003-canonical-storage-postgres-jsonb.md). They are **not current validation, production benchmarks, or proof of Milestone 2 capacity**. No measurements were rerun for this documentation cleanup.

## Methodology and limitations

- Recorded environment: Docker **29.8.2**, CachyOS, **13.49 GiB** host RAM. The BNPB stack was built and started with Docker Compose on branch `docs/report-p1-corrections`; idle readings were taken about **2 minutes** after all services were healthy.
- Memory came from `docker stats --no-stream`; image and binary sizes came from `docker images` and `stat`. Units and recorded precision are retained below (decimal points replace decimal commas).
- Startup measurements followed `docker stop` then `docker start`, with readiness polling every **10 ms**. They include Docker container-start overhead, not just process startup, and do not establish fresh-volume initialization time.
- Third-party comparisons used stock images/default settings (NATS with `-js`), with idle memory sampled **45 seconds** after readiness. FastAPI was a hello-world stand-in, not a rewritten BNPB service; its footprint is only a minimal baseline. Six Python services were not measured.
- Each figure is one sample, without repeated trials or statistical confidence. RabbitMQ's initial readiness-check command failed; the retained RabbitMQ figure came from a rerun and its startup log. NATS readiness was not timed precisely; it was up within a fixed **2 s** probe.
- These were idle and startup observations, not throughput, concurrent-load, failover, durability, or multi-replica tests. Default third-party settings are not equivalent production configurations. The historical notes do not preserve a complete measurement harness or raw output for every figure.

## Idle memory

| Container | Memory |
| :--- | :--- |
| aggregator | 16.62 MiB |
| mock-pvmbg | 9.738 MiB |
| mock-bmkg | 8.926 MiB |
| notifier-consumer | 7.828 MiB |
| dashboard-consumer | 7.809 MiB |
| client-api | 3.105 MiB |
| auth | 1.82 MiB |
| **Total, 7 Go service containers** | **55.8 MiB** |
| canonical-db (`postgres:16-alpine`) | 47.74 MiB |
| redpanda | 352.4 MiB |

Redpanda used `--smp 1 --memory 256M`. In this sample its idle memory exceeded Kafka's **296.6 MiB**; MongoDB's **76.22 MiB** was about 1.6 times PostgreSQL's. These figures do not support large memory-saving claims against Kafka or MongoDB.

## Image and binary sizes

| Service or image | Image size | Binary size |
| :--- | :--- | :--- |
| mock-bmkg | 24.4 MB | 5.06 MB |
| mock-pvmbg | 24.5 MB | 5.07 MB |
| auth | 24.6 MB | 5.14 MB |
| dashboard-consumer | 25.2 MB | 5.62 MB |
| notifier-consumer | 25.2 MB | 5.63 MB |
| aggregator | 32.5 MB | 11.15 MB |
| client-api | 32.6 MB | 5.62 MB |
| postgres:16-alpine | 420 MB | N/A |
| redpanda v24.1.1 | 581 MB | N/A |

## Startup measurements

| Service | Time | Readiness criterion |
| :--- | :--- | :--- |
| mock-pvmbg | 233 ms | `/health` HTTP 200 from host |
| mock-bmkg | 250 ms | `/health` HTTP 200 from host |
| client-api | 272 ms | `/health` HTTP 200 from host |
| aggregator | 304 ms | `/health` HTTP 200 from inside container; host port 8083 did not answer |
| auth | 515 ms | `/health` HTTP 200 from host |
| canonical-db | 542 ms | `pg_isready` |
| redpanda | 1854 ms | Healthy response from `rpk cluster health` |

## Third-party comparisons

| Image | Image size | Ready after | Idle memory |
| :--- | :--- | :--- | :--- |
| mongo:7.0 | 1.18 GB | 1240 ms | 76.22 MiB |
| mysql:8.0 | 1.1 GB | 5411 ms | 385.2 MiB |
| rabbitmq:3.13 | 352 MB | 5178 ms (RabbitMQ log) | 147.5 MiB |
| nats:2.10 (`-js`) | 22.4 MB | Not timed precisely | 7.398 MiB |
| apache/kafka:3.7.0 | 613 MB | 3887 ms | 296.6 MiB |
| FastAPI hello-world (`python:3.12-slim` + FastAPI + Uvicorn) | 224 MB (base: 178 MB) | 929 ms | 31.69 MiB |

Kafka readiness used a broker API-versions response. Readiness criteria were not uniform across products, so startup times are illustrative rather than an equivalent-workload ranking.

## MongoDB transaction findings

Recorded on **2026-10-10**, using `mongo:7.0`:

| Scenario | Recorded result |
| :--- | :--- |
| A. Standalone `mongod`, transaction via `mongosh` session | Failed: `This MongoDB deployment does not support retryable writes` (session-side message, not the transaction-command error) |
| A′. Standalone `mongod`, raw `insert` command with `txnNumber` and `startTransaction: true` | Failed: `MongoServerError: Transaction numbers are only allowed on a replica set member or mongos` |
| B. `--replSet rs0`, before `rs.initiate()` | Failed: `NotWritablePrimary \| not primary` |
| C. **Single-member** replica set, after `rs.initiate()` | Two-document transaction committed: `TX_OK` |

A replica set was required for these multi-document transactions, but multiple nodes were not: one member sufficed. Replica-set initialization was a prerequisite for writes in scenario B. This was one sample on MongoDB 7.0, with no Go-driver test or availability/durability assessment.

The retained [mongo_tx_test.sh](mongo_tx_test.sh) covers **A, B, and C only**. **A′ was a separate manual probe**; the script does not reproduce the exact raw-command error quoted in ADR 003.

### Script safety and requirements

**Use only an isolated, disposable Docker daemon, never a shared or production one.** Review the script and verify the active context first: it unconditionally runs `docker rm -f mtx_a mtx_b` before and after the experiment, deleting any containers with those names and their writable state. Check all containers, including stopped ones, and stop if either name is occupied. There is no cleanup trap; interruption can leave test containers behind.

Requirements: Bash, Docker CLI/daemon, `seq`, `grep`, `sleep`, and cached `mongo:7.0` or registry access to pull it. `mongosh` runs inside the containers on Docker's default network, with no published host ports. Inspect `TX_OK`/`TX_FAIL`: caught transaction errors do not imply a failing shell exit. Clean up only confirmed test-owned resources.

The script was inspected, **not executed**, during this cleanup.
