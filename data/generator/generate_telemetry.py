"""Synthetic IoT telemetry generator.

Generates realistic device telemetry matching the Bronze schema. Produces
partitioned JSON files that Auto Loader (or local batch read) can ingest.

Features:
- Configurable tenant count, device count, event volume
- Injects realistic anomalies: reboots, signal drops, firmware upgrades (SCD2 triggers)
- Injects dirty data for DQ testing: nulls, out-of-range values, future timestamps
- Partitions output by tenant and date for realistic directory layout
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np

DEVICE_MODELS = ["Airwave-X1", "Airwave-X2", "MeshPro-500", "MeshPro-700", "EdgeRouter-Q"]
FIRMWARES = ["3.14.2", "3.14.5", "3.15.0", "3.15.1", "4.0.0"]
GEO_REGIONS = ["NA-East", "NA-West", "EU-Central", "EU-West", "APAC-South", "APAC-East"]
ERROR_CODES = ["ERR_DNS_FAIL", "ERR_DHCP_TIMEOUT", "ERR_RADIO_RESET", "ERR_AUTH_FAIL", "ERR_DISK_IO"]


def _make_event(
    tenant_id: str,
    device_id: str,
    device_model: str,
    firmware: str,
    geo: str,
    event_ts: datetime,
    inject_dirty: bool,
) -> dict:
    """Generate one telemetry event, optionally dirty."""
    signal = int(np.random.normal(-60, 10))
    signal = max(-120, min(0, signal))

    throughput_down = max(0.0, float(np.random.normal(200.0, 60.0)))
    throughput_up = max(0.0, float(np.random.normal(40.0, 15.0)))

    event = {
        "event_id": str(uuid.uuid4()),
        "tenant_id": tenant_id,
        "device_id": device_id,
        "device_model": device_model,
        "firmware_version": firmware,
        "event_timestamp": event_ts.isoformat(),
        "signal_strength_dbm": signal,
        "throughput_mbps_down": round(throughput_down, 2),
        "throughput_mbps_up": round(throughput_up, 2),
        "connected_clients": random.randint(0, 30),
        "cpu_utilization_pct": round(random.uniform(5, 75), 2),
        "memory_utilization_pct": round(random.uniform(20, 85), 2),
        "channel_utilization_pct_2g": round(random.uniform(10, 90), 2),
        "channel_utilization_pct_5g": round(random.uniform(5, 70), 2),
        "uptime_seconds": random.randint(60, 86400 * 30),
        "reboot_flag": random.random() < 0.001,  # 0.1% reboot rate
        "error_codes": (
            random.sample(ERROR_CODES, random.randint(1, 3))
            if random.random() < 0.02
            else []
        ),
        "geo_region": geo,
    }

    if inject_dirty:
        # ~2% dirty data for DQ stress testing
        r = random.random()
        if r < 0.005:
            event["device_id"] = None  # CRITICAL
        elif r < 0.010:
            event["signal_strength_dbm"] = 200  # ERROR (out of range)
        elif r < 0.015:
            event["throughput_mbps_down"] = -10.0  # ERROR (negative)
        elif r < 0.020:
            # Future timestamp
            event["event_timestamp"] = (
                datetime.now(UTC) + timedelta(hours=1)
            ).isoformat()

    return event


def generate(
    output_dir: Path,
    tenants: int,
    devices_per_tenant: int,
    total_events: int,
    days: int,
    inject_dirty: bool = True,
    seed: int = 42,
) -> dict[str, int]:
    """Generate events, bucketed by tenant and date.

    Returns a summary dict of file counts per tenant/date.
    """
    random.seed(seed)
    np.random.seed(seed)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Build device roster
    tenant_devices: dict[str, list[tuple[str, str, str, str]]] = {}
    for ti in range(tenants):
        tenant_id = f"tenant_{chr(ord('a') + ti)}"
        devices = []
        for di in range(devices_per_tenant):
            dev_id = f"dev_{tenant_id}_{di:06d}"
            model = random.choice(DEVICE_MODELS)
            fw = random.choice(FIRMWARES)
            geo = random.choice(GEO_REGIONS)
            devices.append((dev_id, model, fw, geo))
        tenant_devices[tenant_id] = devices

    end = datetime.now(UTC).replace(microsecond=0)
    start = end - timedelta(days=days)
    span_seconds = int((end - start).total_seconds())

    events_per_tenant = total_events // tenants
    summary: dict[str, int] = {}

    for tenant_id, devices in tenant_devices.items():
        # Bucket by date for partitioned layout
        bucketed: dict[str, list[dict]] = {}

        for _ in range(events_per_tenant):
            device_id, model, fw, geo = random.choice(devices)
            ts = start + timedelta(seconds=random.randint(0, span_seconds))
            # Occasionally bump firmware mid-window — triggers SCD2 change downstream
            if random.random() < 0.0001:
                fw = random.choice(FIRMWARES)

            event = _make_event(tenant_id, device_id, model, fw, geo, ts, inject_dirty)
            date_key = ts.strftime("%Y-%m-%d")
            bucketed.setdefault(date_key, []).append(event)

        # Write: one JSONL file per (tenant, date)
        for date_key, events in bucketed.items():
            dir_path = output_dir / f"tenant_id={tenant_id}" / f"event_date={date_key}"
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / f"events_{uuid.uuid4().hex[:8]}.json"
            with file_path.open("w", encoding="utf-8") as fh:
                for ev in events:
                    fh.write(json.dumps(ev, default=str) + "\n")
            summary[f"{tenant_id}/{date_key}"] = len(events)

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic IoT telemetry")
    parser.add_argument("--tenants", type=int, default=3)
    parser.add_argument("--devices-per-tenant", type=int, default=1000)
    parser.add_argument("--events", type=int, default=1_000_000)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--output", type=str, default="data/raw")
    parser.add_argument("--no-dirty", action="store_true", help="Disable dirty data injection")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    output = Path(args.output)
    summary = generate(
        output_dir=output,
        tenants=args.tenants,
        devices_per_tenant=args.devices_per_tenant,
        total_events=args.events,
        days=args.days,
        inject_dirty=not args.no_dirty,
        seed=args.seed,
    )

    total = sum(summary.values())
    print(f"Generated {total:,} events across {len(summary)} partitions into {output}")
    print(f"Sample partitions: {list(summary.items())[:3]}")


if __name__ == "__main__":
    main()
