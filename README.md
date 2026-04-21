# Lakehouse IoT Telemetry Pipeline

> Multi-tenant, production-grade Lakehouse on Databricks + Delta Lake for high-volume IoT device telemetry. Built with PySpark, Auto Loader, Delta Live Tables, and medallion architecture.

[![CI](https://github.com/sushmakl95/lakehouse-iot-telemetry-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/sushmakl95/lakehouse-iot-telemetry-pipeline/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![Spark 3.5](https://img.shields.io/badge/spark-3.5-orange.svg)](https://spark.apache.org/)
[![Delta 3.2](https://img.shields.io/badge/delta-3.2-green.svg)](https://delta.io/)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)

---

## Author

**Sushma K L** — Senior Data Engineer
📍 Bengaluru, India
💼 [LinkedIn](https://www.linkedin.com/in/sushmakl1995/) • 🐙 [GitHub](https://github.com/sushmakl95) • ✉️ sushmakl95@gmail.com

---

## Problem Statement

A global smart-home networking vendor operates **millions of connected devices** across multiple ISPs (tenants). Each device emits telemetry every 30 seconds: signal strength, throughput, client counts, CPU/memory, channel utilization, firmware version. Downstream teams need:

1. **Device health monitoring** — real-time detection of degraded devices
2. **Anomaly detection** — unusual throughput drops, reboot storms
3. **Network performance analytics** — tenant-level SLA reporting
4. **Customer experience insights** — daily/weekly KPI rollups

Traditional batch warehouses choke on the volume (10B+ events/day) and cannot serve near-real-time use cases. This project implements a **multi-tenant Lakehouse** on Databricks that ingests, processes, and serves this data with sub-hour latency while holding costs flat through aggressive optimization.

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  IoT Devices    │────▶│  Cloud Landing   │────▶│  Auto Loader     │────▶│  Bronze (Delta)  │
│  (millions)     │     │  (S3 raw events) │     │  (schema evol.)  │     │  raw append-only │
└─────────────────┘     └──────────────────┘     └──────────────────┘     └────────┬─────────┘
                                                                                    │
                                                                                    ▼
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Gold (Delta)    │◀────│  DLT Pipeline    │◀────│  Silver (Delta)  │◀────│  DQ Expectations │
│  aggregated KPIs │     │  business logic  │     │  cleaned + SCD2  │     │  (quarantine)    │
│  Z-Ordered       │     │  SCD2 dims       │     │  partitioned     │     │                  │
└────────┬─────────┘     └──────────────────┘     └──────────────────┘     └──────────────────┘
         │
         ▼
┌──────────────────┐     ┌──────────────────┐
│  Unity Catalog   │────▶│  BI / ML / API   │
│  (governance)    │     │  consumers       │
└──────────────────┘     └──────────────────┘
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for design decisions, trade-offs, and why each choice was made.

## Key Features

| Area | Implementation |
|---|---|
| **Ingestion** | Databricks Auto Loader with schema inference + evolution, file notification mode for scale |
| **Medallion** | Bronze (raw) → Silver (cleaned, SCD2 dims) → Gold (business aggregates) |
| **Multi-tenancy** | Tenant isolation via partition (`tenant_id`) + Unity Catalog row filters |
| **Data Quality** | DLT `@expect` / `@expect_or_drop` / `@expect_or_fail` with quarantine tables |
| **Performance** | Z-Order on high-cardinality filters, OPTIMIZE + VACUUM cadence, file sizing tuning |
| **Streaming** | Structured Streaming with checkpointing, exactly-once via Delta transactions |
| **Governance** | Unity Catalog table/column lineage, PII tagging, access control patterns |
| **Observability** | Structured JSON logging, custom metrics table, alerting hooks |
| **Cost control** | Job clusters with spot instances, auto-termination, photon enabled on hot paths |
| **CI/CD** | GitHub Actions: ruff lint, pytest unit + integration (Delta local), `databricks bundle validate` |

## Repository Structure

```
lakehouse-iot-telemetry-pipeline/
├── .github/workflows/       # CI pipelines (lint, test, deploy)
├── config/                  # Environment configs (dev/staging/prod)
├── data/generator/          # Synthetic telemetry generator (simulates millions of devices)
├── docs/                    # Architecture, runbooks, cost analysis
├── infra/terraform/         # IaC for S3, IAM, Databricks workspace resources
├── notebooks/               # Databricks notebooks (medallion layers, optimization)
├── scripts/                 # Deployment, maintenance, backfill utilities
├── src/
│   ├── bronze/              # Raw ingestion jobs (Auto Loader)
│   ├── silver/              # Cleaning, conformance, SCD2
│   ├── gold/                # Business KPI aggregations
│   ├── dlt/                 # Delta Live Tables pipeline definitions
│   ├── data_quality/        # DQ rules + expectations framework
│   ├── schemas/             # Typed schemas + contracts
│   └── utils/               # Logging, idempotency, config loader
└── tests/                   # Unit (pyspark-local) + integration (delta-spark)
```

## Quick Start (Local Dev)

Requires: Python 3.11, Java 17, 8GB RAM.

```bash
# 1. Clone and install
git clone https://github.com/sushmakl95/lakehouse-iot-telemetry-pipeline.git
cd lakehouse-iot-telemetry-pipeline
make install

# 2. Generate synthetic telemetry (1M events across 3 tenants)
make generate-data

# 3. Run the bronze → silver → gold pipeline locally (uses delta-spark)
make run-pipeline

# 4. Run tests
make test

# 5. Lint + typecheck
make lint
```

See [docs/LOCAL_DEVELOPMENT.md](docs/LOCAL_DEVELOPMENT.md) for detailed setup including Java, Spark tuning, and troubleshooting.

## Deployment

Terraform provisions the full stack (S3 landing, IAM roles, Databricks workspace objects). GitHub Actions deploys via [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html).

```bash
cd infra/terraform
terraform init
terraform plan -var-file=envs/dev.tfvars
terraform apply -var-file=envs/dev.tfvars
```

> **⚠️ Cloud cost warning**: `terraform apply` creates real paid resources. Review [docs/COST_ANALYSIS.md](docs/COST_ANALYSIS.md) before deploying.

## Performance Benchmarks

Measured on a synthetic 500M-event dataset (50 tenants, 7-day window):

| Optimization | Query Runtime | Cost/Run |
|---|---|---|
| Baseline (no partitioning, no Z-Order) | 412 s | $4.20 |
| + Partition by `event_date` + `tenant_id` | 98 s | $1.10 |
| + Z-Order on `device_id` | 34 s | $0.38 |
| + OPTIMIZE + small-file compaction | 21 s | $0.24 |
| + Photon enabled | 11 s | $0.18 |

**Net**: ~37× faster, ~23× cheaper vs baseline. See [docs/PERFORMANCE.md](docs/PERFORMANCE.md) for methodology.

## Design Decisions

| Decision | Chose | Why |
|---|---|---|
| Table format | Delta Lake | ACID, time travel, schema enforcement, Z-Order, Photon support |
| Ingestion | Auto Loader (not Spark Streaming from S3) | Scales to billions of files, schema evolution, cheaper than listing S3 |
| Orchestration | DLT + Databricks Workflows | Declarative dependencies, built-in DQ, cheaper than external Airflow for this scope |
| Multi-tenancy | Partition by `tenant_id` + UC row filters | Isolation without per-tenant table sprawl |
| SCD2 | MERGE with hash-based change detection | Deterministic, idempotent, replay-safe |
| DQ strategy | DLT expectations + quarantine table | Failures don't break pipeline; bad data inspectable |

## License

MIT — see [LICENSE](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). PRs welcome.
