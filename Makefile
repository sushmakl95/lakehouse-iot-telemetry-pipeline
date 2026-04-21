.PHONY: help install install-dev test test-unit test-integration lint format typecheck security clean generate-data run-pipeline run-bronze run-silver run-gold docs-serve

PYTHON := python3.11
PIP := $(PYTHON) -m pip
VENV := .venv
VENV_BIN := $(VENV)/bin

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)
	$(VENV_BIN)/pip install --upgrade pip setuptools wheel

install: $(VENV)/bin/activate  ## Install runtime dependencies
	$(VENV_BIN)/pip install -e .

install-dev: $(VENV)/bin/activate  ## Install dev + test dependencies
	$(VENV_BIN)/pip install -e ".[dev,databricks]"
	$(VENV_BIN)/pre-commit install

test: test-unit test-integration  ## Run full test suite

test-unit:  ## Run unit tests
	$(VENV_BIN)/pytest tests/unit -v -m unit

test-integration:  ## Run integration tests (requires Java 17)
	$(VENV_BIN)/pytest tests/integration -v -m integration

lint:  ## Run ruff linter
	$(VENV_BIN)/ruff check src tests

format:  ## Auto-format code
	$(VENV_BIN)/ruff format src tests
	$(VENV_BIN)/ruff check --fix src tests

typecheck:  ## Run mypy type checker
	$(VENV_BIN)/mypy src

security:  ## Run security scanners
	$(VENV_BIN)/bandit -r src -c pyproject.toml
	$(VENV_BIN)/detect-secrets scan --baseline .secrets.baseline

generate-data:  ## Generate synthetic IoT telemetry
	$(VENV_BIN)/python -m data.generator.generate_telemetry \
	    --tenants 3 --devices-per-tenant 1000 --events 1000000 --output data/raw

run-pipeline: run-bronze run-silver run-gold  ## Run full medallion pipeline locally

run-bronze:  ## Run bronze layer ingestion
	$(VENV_BIN)/python -m src.bronze.ingest_telemetry --config config/local.yaml

run-silver:  ## Run silver layer transformations
	$(VENV_BIN)/python -m src.silver.transform_telemetry --config config/local.yaml

run-gold:  ## Run gold layer aggregations
	$(VENV_BIN)/python -m src.gold.aggregate_kpis --config config/local.yaml

clean:  ## Clean generated artifacts
	rm -rf build/ dist/ *.egg-info .pytest_cache .coverage htmlcov/
	rm -rf spark-warehouse/ metastore_db/ derby.log
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

clean-data:  ## Clean generated data (destructive)
	rm -rf data/raw data/bronze data/silver data/gold data/generated

docs-serve:  ## Serve docs locally (requires mkdocs)
	$(VENV_BIN)/mkdocs serve

all: install-dev lint typecheck security test  ## Full CI simulation
