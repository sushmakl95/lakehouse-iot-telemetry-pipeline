# Runbook

## Late-arriving telemetry

**Symptom:** Auto Loader backlog rising; bronze has rows but silver doesn't.

1. Check streaming query state: `spark.streams.active` — is any query `STOPPED`?
2. Check S3 for incoming files: `aws s3 ls s3://telemetry-raw/ --recursive | tail`.
3. Check for schema drift: `SELECT * FROM bronze.ingest_errors ORDER BY ts DESC LIMIT 10`.
4. Resume: restart the Auto Loader job; Databricks picks up from last checkpointed offset.

## Auto Loader checkpoint corruption

**Symptom:** `IllegalStateException: checkpointLocation … is not a Delta table`.

1. Back up the checkpoint dir: `dbutils.fs.cp(ckpt, ckpt_bak, recurse=True)`.
2. Verify target table has expected data (bronze should be append-only; no data loss).
3. Reset checkpoint: `dbutils.fs.rm(ckpt, recurse=True)`.
4. Restart with `trigger=Trigger.AvailableNow` for a one-shot catch-up.

## DLT expectations failing (tenant X)

**Symptom:** DLT event log shows `num_expected_records` vs `num_output_records` divergence for tenant X.

1. Query the DLT event log: `SELECT * FROM event_log('pipeline_id') WHERE flow_progress.metrics.num_dropped > 0 ORDER BY timestamp DESC`.
2. Identify which expectation dropped rows: look at `expectation_name`.
3. If genuine bad data — route to quarantine table `bronze.telemetry_quarantine` for tenant-owner review.
4. If schema/contract drift — update the expectation with the new valid values and redeploy via Databricks Asset Bundle.

## Gold mart stale

**Symptom:** Dashboard shows yesterday's numbers.

1. Check job schedule: `databricks jobs get --job-id <gold-job-id>`.
2. Check last run: `databricks jobs runs list --job-id <gold-job-id> --limit 1`.
3. If run is `FAILED` — check driver logs; common cause is cluster quota breach (SPOT termination).
4. If run is `SUCCESS` but data stale — check source freshness in silver; if silver is lagging, fix streaming first.

## Delta table maintenance

Run weekly on silver + gold:

```python
for table in ["silver.device_events", "gold.device_kpis_5min"]:
    spark.sql(f"OPTIMIZE {table}")
    spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
```

## Multi-tenant noisy-neighbour

**Symptom:** One tenant's backlog is starving others on the shared cluster.

1. Check ingestion metrics per tenant: `SELECT tenant_id, count(*) FROM bronze.telemetry_raw WHERE _ingest_ts > now() - INTERVAL 1 HOUR GROUP BY tenant_id ORDER BY 2 DESC`.
2. If one tenant is >50% of volume — move it to its own dedicated pipeline (split Auto Loader path, dedicated job cluster).
3. Update the cluster policy to allow isolation for the heaviest tenants.

## On-call escalation

- First responder: data-platform@example.com
- Escalation (30 min unresolved): data-platform-oncall@example.com + PagerDuty "Data Platform Sev2"
- Link: https://grafana.example.com/d/iot-telemetry (watch p95 silver lag + DLT expectation drops)
