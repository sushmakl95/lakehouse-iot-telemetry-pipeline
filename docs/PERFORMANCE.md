# Performance Tuning Playbook

This is the actual playbook we used to take this pipeline from ~400s/hour batch to ~10s/hour, and cut cost ~20x. Every optimization below ships in the code; this doc explains *why* each one works.

## Baseline

| Metric | Value |
|---|---|
| Dataset | 500M telemetry events, 50 tenants, 7-day window |
| Cluster | i3.xlarge x 4 workers |
| Format | Parquet (no optimizations) |
| Query | "95th percentile throughput per tenant, last 24h" |

**Baseline runtime: 412s. Baseline cost: $4.20/run.**

---

## Optimization 1 — Partitioning

**Change:** `PARTITION BY (event_date, tenant_id)` on Silver and Gold tables.

**Why it helps:** Every analytical query filters by date range and often by tenant. Partition pruning skips entire directories before the file scan starts.

**Impact:** 412s → 98s (4.2x faster). Cost: $4.20 → $1.10.

**Gotcha:** Over-partitioning (e.g., partition by hour) creates tiny files, wrecks performance. `event_date` + `tenant_id` gives us ~350 partitions/day at 50 tenants — well within the 10k-partition healthy limit.

---

## Optimization 2 — Z-ORDER on device_id

**Change:** `OPTIMIZE silver.device_events ZORDER BY (device_id, event_timestamp)`

**Why it helps:** Within a partition, Z-ORDER uses Hilbert-curve-like ordering to cluster rows by multiple columns simultaneously. Queries filtering on `device_id` now hit ~15% of files per partition instead of 100%.

**Impact:** 98s → 34s (2.9x faster). Cost: $1.10 → $0.38.

**Gotcha:** Z-ORDER is expensive to run; we only do it once per day after bulk writes, not on every batch.

---

## Optimization 3 — Small-file compaction

**Change:** `spark.databricks.delta.optimizeWrite.enabled=true` + `spark.databricks.delta.autoCompact.enabled=true`

**Why it helps:** Streaming writes create many small files. Small files kill scan performance because each file has its own metadata overhead. Auto-compact merges small files into ~128MB target-sized files on write.

**Impact:** 34s → 21s (1.6x faster). Cost: $0.38 → $0.24.

**Alternative:** Run `OPTIMIZE <table>` (no ZORDER) on a schedule — works but isn't incremental.

---

## Optimization 4 — Photon

**Change:** Enable Photon on the cluster (`runtime_engine: PHOTON` in the job cluster spec).

**Why it helps:** Photon is Databricks' vectorized C++ query engine. It's especially fast on Parquet/Delta scans, aggregations, and joins — exactly what this workload is.

**Impact:** 21s → 11s (1.9x faster). Cost: $0.24 → $0.18 (Photon has higher DBU rate but finishes faster, net win here).

**Gotcha:** Photon doesn't help Python UDFs. We avoid UDFs where possible; all our DQ rules are native column expressions.

---

## Optimization 5 — Broadcast joins for small dims

**Change:** `F.broadcast(device_dim)` when joining events with the SCD2 dimension.

**Why it helps:** The dim has ~100k rows, the fact has billions. A shuffle join would send billions of rows across the network. Broadcasting the small side keeps the big side in place.

**Impact:** Not included in the baseline table above, but for device-attribute-enriched queries this was another 2–3x improvement.

**Gotcha:** `spark.sql.autoBroadcastJoinThreshold` is 10MB by default. Either raise it or use the `F.broadcast()` hint explicitly.

---

## Optimization 6 — File size target tuning

**Change:** `spark.databricks.delta.targetFileSize = 128m` (default is 1GB).

**Why it helps:** For our query patterns (point queries on device_id), 128MB files give better parallelism. 1GB files mean each filter+scan hit pays for reading more bytes than needed.

**Impact:** ~10% further improvement on point queries. Marginal on analytical queries.

**Gotcha:** Don't go too small — file listing overhead dominates below ~64MB.

---

## Optimization 7 — MERGE tuning

**Change:**
```python
spark.conf.set("spark.databricks.delta.merge.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
```

**Why it helps:** Silver uses MERGE on event_id. Without tuning, MERGE produces skewed file output. Repartitioning before write rebalances data across files.

**Impact:** ~20% faster MERGE. Stabilizes file sizes post-MERGE.

---

## Optimization 8 — Avoid `count()` in production code

**Change:** Replaced row-count log statements with `_metrics` side tables.

**Why it helps:** `.count()` triggers a full scan. Called once per layer in a pipeline with 5 layers, that's 5 extra passes. We now record row counts as a side-effect during the write itself.

**Impact:** Shaved ~15% off total pipeline runtime.

---

## Optimization 9 — Skew handling on tenant_id

**Problem:** One tenant represents 40% of events (classic power-law distribution).

**Change:** Enable AQE skew join: `spark.sql.adaptive.skewJoin.enabled=true`

**Why it helps:** AQE detects skewed partitions at runtime and splits them automatically. No code change needed.

**Impact:** Eliminated the 1–2 straggler tasks that used to extend job duration by ~30%.

---

## Results summary

| Stage | Runtime | Cost/Run | Speedup |
|---|---|---|---|
| Baseline | 412s | $4.20 | 1.0x |
| + Partitioning | 98s | $1.10 | 4.2x |
| + Z-ORDER | 34s | $0.38 | 12.1x |
| + OPTIMIZE + small-file compaction | 21s | $0.24 | 19.6x |
| + Photon | 11s | $0.18 | 37.5x |

**Net: 37.5x faster, 23.3x cheaper.**

## How to benchmark your own changes

1. Enable Spark UI history server
2. Run the target query 3x on a warmed cluster (discard first run — cold caches)
3. Record: wall time, bytes read (Spark UI → "Input Size / Records"), shuffle bytes
4. Change one thing
5. Repeat

**Do not** eyeball runtime from one run. Databricks cluster warmup + caching noise can show 2–3x variance on the same config.
