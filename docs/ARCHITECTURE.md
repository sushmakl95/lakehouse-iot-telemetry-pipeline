# Architecture

## Overview

This project implements a **multi-tenant Lakehouse** for IoT device telemetry. The architecture is a classic **medallion** (Bronze → Silver → Gold) built on Databricks + Delta Lake, with a synthetic data generator for local development and full CI/CD via GitHub Actions + Databricks Asset Bundles.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA PRODUCERS                                 │
│  Millions of smart-home devices emit JSON telemetry every ~30s              │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
                                     ▼
                     ┌───────────────────────────────┐
                     │   S3 Landing (JSON events)    │
                     │   /events/tenant_id=X/        │
                     │   /event_date=YYYY-MM-DD/     │
                     └───────────────┬───────────────┘
                                     │ Auto Loader (file notification)
                                     ▼
    ┌────────────────────────────────────────────────────────────────────┐
    │                         BRONZE (raw)                               │
    │  catalog.bronze.raw_telemetry                                      │
    │  • Append-only, partitioned by (_ingest_date, tenant_id)           │
    │  • Schema-on-write with evolution enabled                          │
    │  • Retention: 90 days (S3 lifecycle)                               │
    └────────────────────────────────────┬───────────────────────────────┘
                                         │ DQ rules (quarantine)
                                         ▼
    ┌────────────────────────────────────────────────────────────────────┐
    │                         SILVER (cleansed)                          │
    │  catalog.silver.device_events                                      │
    │  • Partitioned by (event_date, tenant_id)                          │
    │  • Z-ORDER on (device_id, event_timestamp)                         │
    │  • MERGE on event_id (exactly-once)                                │
    │  • Enriched: health_score, signal_quality_bucket, etc.             │
    │                                                                    │
    │  catalog.silver.device_dim (SCD2)                                  │
    │  • Tracks firmware/model/region changes over time                  │
    │                                                                    │
    │  catalog.silver.quarantine_device_events                           │
    │  • Rows failing DQ, inspectable for root-cause                     │
    └────────────────────────────────────┬───────────────────────────────┘
                                         │
                                         ▼
    ┌────────────────────────────────────────────────────────────────────┐
    │                          GOLD (KPIs)                               │
    │  • device_health_daily   — per-device rollup                       │
    │  • tenant_sla_hourly     — SLA report feed                         │
    │  • reboot_events         — anomaly feed                            │
    │  • network_performance_daily — P50/P95/P99 trending                │
    └────────────────────────────────────┬───────────────────────────────┘
                                         │
                   ┌─────────────────────┼─────────────────────┐
                   ▼                     ▼                     ▼
             Tableau/PowerBI       ML Training           Alerting / API
```

## Design Decisions

### Why Delta Lake?

| Requirement | Delta Lake feature |
|---|---|
| ACID writes at scale | Optimistic concurrency + transaction log |
| Schema enforcement | Strict mode prevents bad writes |
| Schema evolution | `mergeSchema=true` allows additive changes |
| Point-in-time queries | Time travel via version/timestamp |
| Efficient filtering | Z-ORDER clusters data on high-cardinality columns |
| Compaction | `OPTIMIZE` + auto-compact handles the small-file problem |
| Downstream BI/ML | Open format; Photon-accelerated reads |

### Why medallion (Bronze/Silver/Gold)?

- **Separation of concerns**: raw fidelity (Bronze), business rules (Silver), analytics shape (Gold)
- **Replayability**: Bronze is append-only, so we can always re-derive Silver/Gold from scratch
- **Isolation of DQ**: Bronze accepts everything; Silver enforces rules. Quarantined rows don't block ingestion.

### Why Auto Loader instead of Spark Structured Streaming directly from S3?

Auto Loader uses **SQS file notifications** (production) or **directory listing** (dev). At billions-of-files scale, listing is prohibitively expensive — AWS charges per LIST and S3 Consistent List is slow. SQS is the right answer for production.

### Why partition by `(event_date, tenant_id)`?

- `event_date` dominates analytical queries (always a time window filter)
- `tenant_id` creates **physical isolation** per tenant at the file level — supports per-tenant retention, easier GDPR erasure
- Keeping both as partitions stays under the 10,000-partition guideline for reasonable dataset sizes
- High-cardinality columns (`device_id`, `event_timestamp`) go to **Z-ORDER**, not partition columns

### Why Z-ORDER on `device_id`?

Z-ORDER creates a multi-dimensional clustering of file layout. Queries that filter or join on `device_id` touch far fewer files. In our benchmarks this cut query scan by ~6x.

### Why SCD2 for device dimension?

Facts (events) reference attributes that change (firmware, region). Without SCD2, historical analysis uses the *current* attribute value for *past* events — wrong. SCD2 preserves the attribute values that were valid at event time.

### Why quarantine vs. drop?

Dropping bad rows is irreversible and hides upstream bugs. Quarantining lets the data team investigate, contact device vendors if needed, and potentially re-process after a fix. The quarantine table has a `_dq_failed_rules` array column so investigators can slice by rule.

### Why DLT AND standalone PySpark jobs?

- **DLT** (Delta Live Tables) is the Databricks-native path: declarative, auto-managed dependencies, expectation framework, auto-scaling. Production default.
- **Standalone PySpark** jobs run identical logic locally, in CI, or on non-Databricks compute. Required for unit/integration testing without a Databricks account.
- Both paths share the same schemas and DQ rule definitions — single source of truth.

## Trade-offs

| Decision | Trade-off |
|---|---|
| MERGE for Silver upserts | Higher write cost vs. append; needed for exactly-once |
| Partition by tenant | File sprawl if tenants are tiny; fine at our scale |
| Hash-based SCD2 | Adds one SHA2 per row; dramatically simpler than attr-by-attr compare |
| Quarantine table | Extra storage cost; worth it for observability |
| Autoloader `addNewColumns` mode | Pipeline restart required on new column; safer than `rescue` mode |

## Operational Concerns

- **Idempotency**: every layer tracks `batch_id` in a Delta control table; re-runs are safe
- **Observability**: structured JSON logs → CloudWatch/Log Analytics; DLT emits event logs to UC
- **Alerting**: job failures email oncall via Databricks email notifications; DLT expectations emit metrics consumable by Datadog/PagerDuty
- **Cost**: spot instances + auto-termination + Photon on hot paths; see `COST_ANALYSIS.md`
- **DR**: S3 versioning on Bronze bucket; Delta time travel for 30-day recovery on Silver/Gold

## Reference

- [Delta Lake docs](https://docs.delta.io/latest/index.html)
- [Auto Loader production guide](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/production.html)
- [DLT programming guide](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Unity Catalog best practices](https://docs.databricks.com/en/data-governance/unity-catalog/best-practices.html)
