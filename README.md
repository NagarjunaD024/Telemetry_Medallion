# Telemetry Lakehouse

A Bronze → Silver → Gold pipeline that turns sparse, nested EV-fleet telemetry
(`test_data.json`) into a clean, analytics-ready 1-minute dataset.

**Dataset:** 12 vehicles · 720,139 raw documents · ~4 months (2021).
Each document is one *batch* from one vehicle many signals bundled together,
each carrying its own timestamp (sent ~10–15 s after measurement).

---

## Pipeline

| Layer | Script | Does | Output |
|-------|--------|------|--------|
| **Bronze** | `bronze.py` | Land raw docs as partitioned Parquet; keep payload verbatim | `bronze/device_id=*/event_date=*/` |
| **Silver** | `silver.py` | Explode nested arrays → rows; validate; compact | `gps / climate / motor / rejected .parquet` |
| **Gold** | `gold.py` | Resample to 1-min buckets; derive trips, charging, distance | `telemetry_1min.parquet` |

```
python src/bronze.py     # ~720K docs → partitioned Parquet
python src/silver.py     # → 4 clean tables (5.6M gps, 222K climate, 5.6M motor, 85K rejected)
python src/gold.py       # → 240,747 minute-rows × 18 columns
```

**Setup**
```
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# place test_data.json in Data/raw/  (data is gitignored)
```

---

## Key design decisions

**Bronze — keep the payload sealed.** 

**Silver — table shape follows the data.** 

**Gold — aggregate by physical type.** 

---

## Scope & what I'd add at scale

The  pipeline is built for a static file. A few things were **consciously left out** because the data didn't
justify the complexity but here's where they'd go:

- **Table format → Delta / Iceberg.** Replaces the hand-rolled pieces
  (idempotent rebuild, compaction, schema guards) with ACID, time travel,
  `MERGE` upserts, and native schema evolution for mixed firmware.
- **Incremental load.** The `device_id/event_date` layout is already
  incremental-ready process only new partitions instead of full rebuilds.

---

