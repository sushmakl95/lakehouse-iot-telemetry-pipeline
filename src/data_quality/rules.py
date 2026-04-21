"""Data Quality rule engine for Bronze → Silver transitions.

Approach:
- Each rule is a (name, predicate, severity) triple.
- Predicates are PySpark column expressions (side-effect free).
- Rows failing a rule are either dropped, quarantined, or fail the job based on severity.

Why this over Great Expectations or Deequ here?
- Zero extra deps; keeps the container small for job clusters.
- Rules are version-controlled alongside the transforms they guard.
- DLT expectations consume the same predicate definitions (see src/dlt/*).

Severities:
- CRITICAL: fail the job
- ERROR:    quarantine the row, continue
- WARN:     log the row, keep it
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARN = "WARN"


@dataclass(frozen=True)
class DQRule:
    name: str
    predicate: Column  # column expression that evaluates True for VALID rows
    severity: Severity
    description: str = ""


@dataclass
class DQResult:
    total: int = 0
    passed: int = 0
    quarantined: int = 0
    critical_failures: int = 0
    rule_violations: dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Rule library: IoT telemetry
# ---------------------------------------------------------------------------
TELEMETRY_RULES: list[DQRule] = [
    DQRule(
        name="device_id_not_null",
        predicate=F.col("device_id").isNotNull() & (F.trim(F.col("device_id")) != ""),
        severity=Severity.CRITICAL,
        description="device_id is the join key; null breaks every downstream query",
    ),
    DQRule(
        name="tenant_id_not_null",
        predicate=F.col("tenant_id").isNotNull(),
        severity=Severity.CRITICAL,
        description="tenant_id is the partition key and security boundary",
    ),
    DQRule(
        name="event_timestamp_not_null",
        predicate=F.col("event_timestamp").isNotNull(),
        severity=Severity.CRITICAL,
        description="timestamp drives partitioning and SCD2; required",
    ),
    DQRule(
        name="event_timestamp_not_future",
        predicate=F.col("event_timestamp") <= F.current_timestamp() + F.expr("INTERVAL 5 MINUTES"),
        severity=Severity.ERROR,
        description="Device clocks drift; reject events >5min in the future",
    ),
    DQRule(
        name="signal_strength_in_range",
        predicate=(
            F.col("signal_strength_dbm").isNull()
            | F.col("signal_strength_dbm").between(-120, 0)
        ),
        severity=Severity.ERROR,
        description="WiFi dBm is always negative; 0 is physically impossible",
    ),
    DQRule(
        name="throughput_non_negative",
        predicate=(
            (F.col("throughput_mbps_down").isNull() | (F.col("throughput_mbps_down") >= 0))
            & (F.col("throughput_mbps_up").isNull() | (F.col("throughput_mbps_up") >= 0))
        ),
        severity=Severity.ERROR,
        description="Negative throughput indicates a counter overflow / bad reading",
    ),
    DQRule(
        name="cpu_pct_in_range",
        predicate=F.col("cpu_utilization_pct").isNull()
        | F.col("cpu_utilization_pct").between(0, 100),
        severity=Severity.WARN,
        description="CPU % outside [0,100] indicates sensor calibration issues",
    ),
    DQRule(
        name="memory_pct_in_range",
        predicate=F.col("memory_utilization_pct").isNull()
        | F.col("memory_utilization_pct").between(0, 100),
        severity=Severity.WARN,
        description="Memory % outside [0,100] indicates sensor calibration issues",
    ),
    DQRule(
        name="connected_clients_non_negative",
        predicate=F.col("connected_clients").isNull() | (F.col("connected_clients") >= 0),
        severity=Severity.ERROR,
        description="Client count must be non-negative",
    ),
]


def apply_rules(
    df: DataFrame, rules: list[DQRule]
) -> tuple[DataFrame, DataFrame, DQResult]:
    """Apply rules, split into (clean, quarantined) and return metrics.

    Returns:
        clean_df: rows passing all ERROR/CRITICAL rules
        quarantine_df: rows failing ERROR/CRITICAL with _dq_failed_rules array
        result: DQResult metrics
    """
    # Add a column per rule marking pass/fail
    annotated = df
    rule_cols: list[str] = []
    for rule in rules:
        col_name = f"_dq_{rule.name}"
        annotated = annotated.withColumn(col_name, rule.predicate)
        rule_cols.append(col_name)

    # Array of failed rule names per row
    blocking_rules = [r for r in rules if r.severity in (Severity.CRITICAL, Severity.ERROR)]
    blocking_cols = [f"_dq_{r.name}" for r in blocking_rules]

    if blocking_cols:
        failed_array_expr = F.array_compact(
            F.array(
                *[
                    F.when(~F.col(col), F.lit(col.replace("_dq_", ""))).otherwise(F.lit(None))
                    for col in blocking_cols
                ]
            )
        )
    else:
        failed_array_expr = F.array()

    annotated = annotated.withColumn("_dq_failed_rules", failed_array_expr)

    # Split
    is_clean = F.size(F.col("_dq_failed_rules")) == 0
    clean_df = annotated.filter(is_clean).drop(*rule_cols, "_dq_failed_rules")
    quarantine_df = annotated.filter(~is_clean).drop(*rule_cols)

    # Metrics (uses cache to avoid re-computing the DAG)
    annotated.cache()
    total = annotated.count()
    passed = annotated.filter(is_clean).count()

    # Per-rule violation counts
    violations: dict[str, int] = {}
    for rule in rules:
        violations[rule.name] = annotated.filter(~F.col(f"_dq_{rule.name}")).count()

    critical_failures = sum(
        violations[r.name] for r in rules if r.severity == Severity.CRITICAL
    )

    annotated.unpersist()

    return (
        clean_df,
        quarantine_df,
        DQResult(
            total=total,
            passed=passed,
            quarantined=total - passed,
            critical_failures=critical_failures,
            rule_violations=violations,
        ),
    )
