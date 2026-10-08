.DEFAULT_GOAL := help

SHELL := /bin/bash

# Detect Docker permissions: auto-fallback to passwordless sudo if needed
DOCKER_COMPOSE ?= $(shell docker info >/dev/null 2>&1 && echo "docker compose" || echo "sudo docker compose")
DOCKER ?= $(shell docker info >/dev/null 2>&1 && echo "docker" || echo "sudo docker")

.PHONY: help up down restart logs build test demo-p1 demo-p2 demo-p3 demo-p4 demo-p5 test-observability load-test report

## Display available make targets
help:
	@echo "========================================================================"
	@echo "BNPB Disaster Early Warning & Hazard Aggregation System"
	@echo "========================================================================"
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  up           Start all services in detached mode via Docker Compose"
	@echo "  down         Stop all services and remove containers, networks, volumes"
	@echo "  restart      Restart all running Docker Compose services"
	@echo "  logs         Tail follow logs from all Docker Compose services"
	@echo "  build        Build or rebuild Docker Compose service images"
	@echo "  test         Run unit and integration tests across all Go services"
	@echo "  demo-p1      Execute Problem 1 demo (PVMBG schema evolution & confidence level)"
	@echo "  demo-p2      Execute Problem 2 demo (PVMBG outage simulation & degraded response)"
	@echo "  demo-p3      Execute Problem 3 demo (cross-credential rejection + auth scopes + TTL/refresh)"
	@echo "  demo-p4      Execute Problem 4 demo (independent container restart & flexible JSONB storage)"
	@echo "  demo-p5      Execute Problem 5 demo (Pub-Sub event fan-out & consumer recovery)"
	@echo "  load-test    Run k6 load test against Client API (50 VUs, 60s)"
	@echo "  report       Compile modular LaTeX report to PDF via tectonic"
	@echo "========================================================================"

## Start services in background
up:
	$(DOCKER_COMPOSE) up -d

## Stop services and remove volumes
down:
	$(DOCKER_COMPOSE) down -v

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
		$(DOCKER) run --rm -i --network host -e AUTH_INTERNAL_CLIENT_SECRET="$$AUTH_INTERNAL_CLIENT_SECRET" -e AUTH_INTERNAL_CLIENT_ID="$${AUTH_INTERNAL_CLIENT_ID:-test-internal-client}" grafana/k6 run - < ./scripts/load_test.js; \
	fi

## Build modular LaTeX report using tectonic
report:
	@if command -v tectonic >/dev/null 2>&1; then \
		echo "[report] Compiling modular LaTeX report via tectonic..."; \
		tectonic docs/report/main.tex; \
		cp docs/report/main.pdf docs/report/IF4031_M1_AhhAldoyTolong.pdf; \
		echo "[report] Successfully generated:"; \
		echo "         - docs/report/main.pdf"; \
		echo "         - docs/report/IF4031_M1_AhhAldoyTolong.pdf"; \
	else \
		echo "[report] ERROR: tectonic is not installed. Install via: sudo pacman -S tectonic" >&2; \
		exit 1; \
	fi

