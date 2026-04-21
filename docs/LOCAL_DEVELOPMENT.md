# Local Development

## Prerequisites

| Requirement | Version | Why |
|---|---|---|
| Python | 3.11 | `delta-spark` 3.2 supports 3.9–3.11; 3.12+ has compat issues |
| Java | 17 | Required by Spark 3.5 |
| Make | any | Convenience wrapper |
| Memory | 8GB+ free | Spark driver defaults to 4GB here |
| Disk | 5GB+ free | Generator produces ~1GB per million events; Delta overhead ~2x |

## Setup

```bash
# 1. Clone
git clone https://github.com/sushmakl95/lakehouse-iot-telemetry-pipeline.git
cd lakehouse-iot-telemetry-pipeline

# 2. Install Java 17 (if not already)
# macOS:  brew install openjdk@17
# Ubuntu: sudo apt install openjdk-17-jdk
# Verify: java -version

# 3. Install Python deps
make install-dev

# 4. Activate the venv
source .venv/bin/activate
```

## Run the full pipeline locally

```bash
# Generate 1M events across 3 tenants, 7 days
make generate-data

# Run bronze → silver → gold
make run-pipeline

# Or step-by-step
make run-bronze
make run-silver
make run-gold
```

Data lands in `data/bronze`, `data/silver`, `data/gold` as local Delta tables. Explore them with the PySpark REPL:

```bash
source .venv/bin/activate
pyspark \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

```python
>>> spark.read.table("local_catalog.gold.tenant_sla_hourly").show()
```

## Run tests

```bash
# Unit tests only (fast)
make test-unit

# Full suite including integration (requires Java 17 on PATH)
make test

# With coverage
pytest --cov=src --cov-report=html
open htmlcov/index.html
```

## Common issues

### `java.lang.NoClassDefFoundError: org/apache/spark/sql/delta/...`

You didn't include the Delta package when starting Spark. Use the `--packages` flag or ensure the Delta JARs are on the classpath. `get_spark_session()` handles this automatically — if you're building Spark manually, match its config.

### `Task not serializable` or `PicklingError`

Usually means you're closing over a non-serializable object in a UDF. Our code avoids UDFs specifically to dodge this. If you add one, use `pyspark.sql.functions` native expressions first.

### OOM in driver on `collect()` or `toPandas()`

Don't. We use `display()` in notebooks and `.limit(N).collect()` in code deliberately. If you need to pull large results, use `spark.table(x).toPandas()` only after a heavy `.filter()` or `.groupBy()`.

### "Database already exists" / "Table already exists" in tests

`conftest.py` creates a fresh warehouse dir per session. If you hit this, run `make clean` (removes `spark-warehouse/` and `metastore_db/`).

### Spark UI not accessible

`conftest.py` disables the UI to keep tests fast. For manual runs, remove `spark.ui.enabled=false`.

## Editor setup

### VS Code

Recommended extensions:
- Python (ms-python.python)
- Ruff (charliermarsh.ruff)
- MyPy (matangover.mypy)

Workspace settings (`.vscode/settings.json`):
```json
{
  "python.defaultInterpreterPath": ".venv/bin/python",
  "python.testing.pytestEnabled": true,
  "python.testing.pytestArgs": ["tests"],
  "[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff",
    "editor.formatOnSave": true
  }
}
```

### PyCharm

- Set Python interpreter to `.venv/bin/python`
- Mark `src` as Sources Root
- Mark `tests` as Test Sources Root

## Contributing workflow

```bash
# 1. Branch
git checkout -b feat/my-change

# 2. Code + test
# ...
make format
make lint
make typecheck
make test

# 3. Commit (pre-commit runs automatically)
git add .
git commit -m "feat: add new DQ rule for X"

# 4. Push + open PR
git push origin feat/my-change
```

GitHub Actions will run the full CI (lint + security + unit + integration + Terraform validate) on every push.
