# Cost Analysis

## Monthly estimate (prod, 500M events/day, 50 tenants)

| Component | Monthly cost (USD) | Notes |
|---|---|---|
| Databricks Jobs Compute (hourly pipeline, spot) | $620 | 4x i3.xlarge, 15 min/hr avg, Photon ON |
| Databricks DLT (for the DLT path) | $180 | Continuous mode off; triggered every hour |
| S3 storage (Bronze 90d + Silver/Gold 365d) | $140 | ~6 TB total with compression |
| S3 requests (PUT/GET/LIST) | $35 | Auto Loader uses SQS, not listing |
| SQS (Auto Loader notifications) | $5 | 30M messages/month |
| CloudWatch logs | $25 | Structured JSON logs, 7-day retention |
| Data transfer | $15 | Intra-region, minimal |
| **Total** | **~$1,020** | |

## Cost optimization checklist (already applied)

- [x] Spot instances with on-demand fallback (`SPOT_WITH_FALLBACK`)
- [x] Cluster auto-termination after 10 min idle
- [x] Photon enabled (higher DBU rate, but cuts runtime ~2x — net positive on steady workloads)
- [x] S3 lifecycle rules: Bronze events → 90 days, checkpoints → 30 days, quarantine → 180 days
- [x] Auto Loader file-notification mode (not list-based) — cheaper at scale
- [x] `noncurrent_version_expiration = 30 days` on versioned buckets
- [x] Abort incomplete multipart uploads after 7 days
- [x] VACUUM retention at 168h (7 days) — balances time-travel window vs. storage cost

## Warning knobs

| Knob | Setting | Why |
|---|---|---|
| `cluster.autoscale.max_workers` | 8 | Hard cap to prevent runaway autoscale cost |
| `cluster.autotermination_minutes` | 10 | Don't pay for idle clusters |
| S3 bucket versioning | Enabled + 30d expiration | Disaster recovery without unbounded storage cost |
| Job `max_concurrent_runs` | 1 | Prevent overlapping runs from spawning parallel clusters |

## Scaling projections

| Event volume | Estimated monthly cost |
|---|---|
| 100M/day | $350 |
| 500M/day | $1,020 |
| 1B/day | $1,700 |
| 5B/day | $6,500 |

Cost scales roughly linearly with event volume (Photon + partitioning prevents superlinear blowup). The biggest cost-per-event tier is Bronze storage; reducing the Bronze retention from 90d → 30d saves ~$60/month at 500M/day.

## ⚠️ Before you `terraform apply`

Running the Terraform in `infra/terraform` creates **real paid resources** in AWS and Databricks. At minimum:

1. Review the `.tfvars` file for bucket naming conflicts
2. Verify your AWS account has tag-based cost allocation enabled
3. Set up AWS Budgets with alerts at $100 and $500
4. Plan to `terraform destroy` after testing — this is a portfolio/demo project, not production

For the simplest cost-free path: **run the pipeline locally** using `make generate-data && make run-pipeline`. The Terraform code is included as a reference artifact showing IaC best practices, not as something you must deploy.
