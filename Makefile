# dbt-tpch Makefile
# Usage: make help

SF ?= 1

# --- Postgres ---

.PHONY: pg-up pg-down pg-reset pg-logs pg-shell pg-load

pg-up: ## Start PostgreSQL server
	docker compose up -d postgres
	@echo "Waiting for PostgreSQL to be ready..."
	@docker compose exec postgres sh -c 'until pg_isready -U dbt -d tpch; do sleep 1; done'
	@echo "PostgreSQL is ready on localhost:5432"

pg-down: ## Stop PostgreSQL server
	docker compose down

pg-reset: ## Stop PostgreSQL and remove data volume
	docker compose down -v

pg-logs: ## Tail PostgreSQL logs
	docker compose logs -f postgres

pg-shell: ## Open psql shell
	docker compose exec postgres psql -U dbt -d tpch

pg-load: ## Generate TPC-H data and load into Postgres (SF=$(SF))
	uv run python scripts/generate_data_postgres.py --sf $(SF)

# --- DuckDB ---

.PHONY: duckdb-load

duckdb-load: ## Generate TPC-H data into DuckDB (SF=$(SF))
	uv run python scripts/generate_data.py --sf $(SF)

# --- dbt (DuckDB) ---

.PHONY: dbt-deps dbt-build dbt-build-base dbt-build-current dbt-test

dbt-deps: ## Install dbt packages
	uv run dbt deps

dbt-build: ## Build dbt models (DuckDB dev target)
	uv run dbt build --target dev

dbt-build-base: ## Build dbt models into base schema (DuckDB)
	uv run dbt build --target base

dbt-build-current: ## Build dbt models into current schema (DuckDB)
	uv run dbt build --target current

dbt-test: ## Run dbt tests (DuckDB dev target)
	uv run dbt test --target dev

# --- dbt (Postgres) ---

.PHONY: pg-dbt-build pg-dbt-build-base pg-dbt-build-current pg-dbt-test

pg-dbt-build: ## Build dbt models (Postgres pg-dev target)
	uv run dbt build --target pg-dev

pg-dbt-build-base: ## Build dbt models into base schema (Postgres)
	uv run dbt build --target pg-base

pg-dbt-build-current: ## Build dbt models into current schema (Postgres)
	uv run dbt build --target pg-current

pg-dbt-test: ## Run dbt tests (Postgres pg-dev target)
	uv run dbt test --target pg-dev

# --- Setup ---

.PHONY: setup setup-pg

setup: dbt-deps duckdb-load dbt-build ## Full DuckDB setup: deps + data + build

setup-pg: dbt-deps pg-up pg-load pg-dbt-build ## Full Postgres setup: deps + server + data + build

# --- Help ---

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
