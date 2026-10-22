# Data Engineer từ A–Z — shared environment
# Usage:
#   make up                    # postgres + minio + jupyter (part 1–2, ~3 GB RAM)
#   make up PROFILE=streaming  # + kafka, schema-registry, kafka-ui, debezium
#   make up PROFILE=spark      # + spark master/worker
#   make up PROFILE=all        # everything (airflow, marquez, trino…) ~12 GB RAM
#   make down | reset | seed | logs | test | lint

SHELL := /bin/bash
PROFILE ?= core
COMPOSE := docker compose --env-file .env
UV ?= uv

.PHONY: help env up down reset seed logs ps test lint fmt install dbt-parse smoke generate

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from .env.example if missing
	@test -f .env || cp .env.example .env

up: env ## Start the environment (PROFILE=core|streaming|spark|analytics|notebook|all)
ifeq ($(PROFILE),all)
	$(COMPOSE) --profile core --profile streaming --profile spark --profile orchestration --profile analytics --profile notebook up -d
else ifeq ($(PROFILE),core)
	$(COMPOSE) --profile core --profile notebook up -d
else
	$(COMPOSE) --profile core --profile $(PROFILE) up -d
endif
	@echo "✅ Environment up. Run 'make ps' to see services, 'make logs' to tail."

down: env ## Stop all services (keeps volumes)
	$(COMPOSE) --profile core --profile streaming --profile spark --profile orchestration --profile analytics --profile notebook down

reset: env ## Stop everything AND delete volumes (fresh start)
	$(COMPOSE) --profile core --profile streaming --profile spark --profile orchestration --profile analytics --profile notebook down -v --remove-orphans
	rm -rf lake-local

seed: env ## Re-seed Postgres from data/samples (safe to re-run)
	$(COMPOSE) exec -T postgres psql -U $${POSTGRES_USER:-coffee} -d $${POSTGRES_DB:-coffee_pos} -f /docker-entrypoint-initdb.d/01_schema.sql
	$(COMPOSE) exec -T postgres psql -U $${POSTGRES_USER:-coffee} -d $${POSTGRES_DB:-coffee_pos} -f /docker-entrypoint-initdb.d/02_seed.sql

logs: env ## Tail logs of all running services
	$(COMPOSE) logs -f --tail=100

ps: env ## List running services
	$(COMPOSE) ps

install: ## Install Python packages (generator, ingestion) + dev tools into .venv
	$(UV) sync --all-packages --group dev

generate: ## Regenerate committed samples with the generator (deterministic)
	$(UV) run coffeegen pos --days 7 --stores 30 --seed 42 --out data/samples
	$(UV) run coffeegen app-events --days 7 --seed 42 --out data/samples
	$(UV) run coffeegen iot --days 1 --machines 60 --hz 0.02 --seed 42 --out data/samples
	$(UV) run python data/samples/make_seed_sql.py

test: ## Run unit tests
	$(UV) run pytest

lint: ## Ruff lint + format check
	$(UV) run ruff check .
	$(UV) run ruff format --check .

fmt: ## Auto-fix lint + format
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

dbt-parse: ## Parse the dbt project with the duckdb (CI) profile — no services needed
	cd platform/dbt && $(UV) run dbt deps --profiles-dir . --target ci && $(UV) run dbt parse --profiles-dir . --target ci

smoke: ## Smoke-run the ingestion pipeline on committed samples → ./lake-local
	$(UV) run coffee-ingest run --source data/samples --lake ./lake-local --date 2026-01-02
