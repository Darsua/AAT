.DEFAULT_GOAL := help

SHELL := /bin/bash

# Detect Docker permissions: auto-fallback to passwordless sudo if needed
DOCKER_COMPOSE ?= $(shell docker info >/dev/null 2>&1 && echo "docker compose" || echo "sudo docker compose")
DOCKER ?= $(shell docker info >/dev/null 2>&1 && echo "docker" || echo "sudo docker")

.PHONY: help up up-sharded down down-sharded restart logs build test k8s-validate k8s-deploy catalog-bench audit-verify demo-p1 demo-p2 demo-p3 demo-p4 demo-p5 test-observability load-test report

## Display available make targets
help:
	@echo "========================================================================"
	@echo "BNPB Disaster Early Warning & Hazard Aggregation System"
	@echo "========================================================================"
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  up            Start all services in detached mode via Docker Compose"
	@echo "  up-sharded    Start 3-node sharded Postgres + 3-replica HA Aggregator stack"
	@echo "  down          Stop all services and remove containers, networks, volumes"
	@echo "  down-sharded  Stop sharded Compose stack and remove volumes"
	@echo "  restart       Restart all running Docker Compose services"
	@echo "  logs          Tail follow logs from all Docker Compose services"
	@echo "  build         Build or rebuild Docker Compose service images"
	@echo "  test          Run unit and integration tests across all Go services"
	@echo "  k8s-validate  Render and validate Kubernetes manifests via kubectl kustomize"
	@echo "  k8s-deploy    Deploy the full M2 Kubernetes stack with one command"
	@echo "  catalog-bench Load historical earthquake catalog + spike skew benchmark (JSON)"
	@echo "  audit-verify  Run independent SHA-256 checksum & Audit API cross-verification"
	@echo "  demo-p1       Execute Problem 1 demo (PVMBG schema evolution & confidence level)"
	@echo "  demo-p2       Execute Problem 2 demo (PVMBG outage simulation & degraded response)"
	@echo "  demo-p3       Execute Problem 3 demo (cross-credential rejection + auth scopes + TTL/refresh)"
	@echo "  demo-p4       Execute Problem 4 demo (independent container restart & flexible JSONB storage)"
	@echo "  demo-p5       Execute Problem 5 demo (Pub-Sub event fan-out & consumer recovery)"
	@echo "  load-test     Run k6 load test against Client API (50 VUs + 5 direct PVMBG VUs, 60s)"
	@echo "  report        Compile modular LaTeX report to PDF via tectonic"
	@echo "========================================================================"

## Start services in background
up:
	$(DOCKER_COMPOSE) up -d --build

## Start sharded 3-node Postgres + 3-replica HA Aggregator stack
up-sharded:
	$(DOCKER_COMPOSE) -f docker-compose.yml -f docker-compose.sharded.yml up --build -d

## Stop services and remove volumes
down:
	$(DOCKER_COMPOSE) down -v

## Stop sharded Compose stack and remove volumes
down-sharded:
	$(DOCKER_COMPOSE) -f docker-compose.yml -f docker-compose.sharded.yml down -v

## Restart services
restart:
	$(DOCKER_COMPOSE) restart

## Stream logs from all services
logs:
	$(DOCKER_COMPOSE) logs -f

## Build container images
build:
	$(DOCKER_COMPOSE) build

## Run Go unit tests
test:
	go test -v ./services/aggregator/... ./services/auth/... ./services/client-api/... ./services/consumers/dashboard/... ./services/consumers/notifier/... ./services/mock-bmkg/... ./services/mock-pvmbg/...

## Validate rendered Kubernetes manifests
k8s-validate:
	@chmod +x ./scripts/deploy_k8s.sh
	./scripts/deploy_k8s.sh --dry-run

## Deploy Kubernetes workloads cleanly in one command
k8s-deploy:
	@chmod +x ./scripts/deploy_k8s.sh
	./scripts/deploy_k8s.sh

## Load historical catalog and measure shard/node skew
catalog-bench:
	cd services/aggregator && go run ./cmd/catalog-loader --in-memory --json

## Run independent SHA-256 checksum and Audit API cross-verification
audit-verify:
	python3 scripts/verify_audit_and_catalog.py --self-test --json

## Problem 1: Schema change demonstration
demo-p1:
	@chmod +x ./scripts/test_schema_change.sh
	./scripts/test_schema_change.sh

## Problem 2: Outage simulation demonstration
demo-p2:
	@chmod +x ./scripts/test_outage.sh
	./scripts/test_outage.sh

## Problem 3: Scope-based access control demonstration (includes cross-credential
## rejection and a real-time wait for natural token TTL expiration; set
## SKIP_TTL_WAIT=1 to skip the wait during quick iteration)
demo-p3:
	@chmod +x ./scripts/test_cross_credential.sh ./scripts/test_auth_scopes.sh
	./scripts/test_cross_credential.sh
	./scripts/test_auth_scopes.sh

## Problem 4: Independent restart & flexible storage demonstration
demo-p4:
	@chmod +x ./scripts/test_storage_restart.sh
	./scripts/test_storage_restart.sh

## Problem 5: Pub-Sub event fan-out & consumer recovery demonstration
demo-p5:
	@chmod +x ./scripts/test_consumer_recovery.sh
	./scripts/test_consumer_recovery.sh

## Audit HTTP and Kafka observability paths
test-observability:
	@chmod +x ./scripts/test_observability.sh
	./scripts/test_observability.sh

## Load test using k6 (native binary or container fallback)
load-test:
	@if [ -z "$$AUTH_INTERNAL_CLIENT_SECRET" ]; then \
		echo "[load-test] ERROR: AUTH_INTERNAL_CLIENT_SECRET environment variable is required" >&2; \
		exit 1; \
	fi
	@if command -v k6 >/dev/null 2>&1; then \
		echo "[load-test] Running k6 load test via local binary..."; \
		k6 run ./scripts/load_test.js; \
	else \
		echo "[load-test] k6 binary not found locally. Running via Docker grafana/k6..."; \
		$(DOCKER) run --rm -i --network host -e AUTH_INTERNAL_CLIENT_SECRET="$$AUTH_INTERNAL_CLIENT_SECRET" -e AUTH_INTERNAL_CLIENT_ID="$${AUTH_INTERNAL_CLIENT_ID:-test-internal-client}" -e PVMBG_TOKEN="$$PVMBG_TOKEN" -e LOAD_DURATION="$${LOAD_DURATION:-60s}" grafana/k6 run - < ./scripts/load_test.js; \
	fi

## Build the M1 LaTeX report using tectonic
report:
	@set -e; \
	if command -v tectonic >/dev/null 2>&1; then \
		echo "[report] Compiling the M1 LaTeX report via tectonic..."; \
		tectonic docs/report/m1/main.tex; \
		cp docs/report/m1/main.pdf docs/report/m1/IF4031_M1_AhhAldoyTolong.pdf; \
		echo "[report] Submission PDF: docs/report/m1/IF4031_M1_AhhAldoyTolong.pdf"; \
	else \
		echo "[report] ERROR: tectonic is not installed. Install via: sudo pacman -S tectonic" >&2; \
		exit 1; \
	fi

