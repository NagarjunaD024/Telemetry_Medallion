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

**Run the whole pipeline**
```
python main.py        # runs Bronze → Silver → Gold in sequence
```

Or run a single layer:
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

## Exercise answers

### Exercise 1 — Bronze Layer

**What fields would you extract from each document?**
Only identifier + partition + lineage fields: `device_id` (from `metaObd.id`),
`batch_ts` and `event_date` (from `metaObd.timestamp`), and `ingestion_ts`.
The entire original document is kept verbatim in `raw_payload`. 

**How would you organize the Parquet files?**
Hive-partitioned Parquet under `bronze/device_id=*/event_date=*/`. The file is
streamed in 50k-document chunks so ingestion stays memory-bounded, then each
partition is written as Parquet (Snappy-compressed).

**Which partitioning strategy would you use and why?**
`device_id` first, then `event_date`. The dominant fleet query is *"vehicle X
over a time window"*, so leading with vehicle prunes the largest filter first,
and date prunes within it. With 12 vehicles × ~120 days this gives a healthy
partition count without a small-files explosion. (Colons are stripped from the
MAC so partition values are URL-safe for Spark.)

*Output: Parquet files partitioned by vehicle then date.*

### Exercise 2 — Silver Layer

**How would you transform nested arrays into rows?**
Each signal is `[value, …, timestamp]`. I explode every array element into a row
keyed on the signal's **own** timestamp (not the batch timestamp), preserving
the true measurement time.

**How would you model separate GPS, Climate, and Motor tables?**
Table shape follows the data:
- **GPS → wide.** lat/lon/speed/altitude share timestamps, so they pivot into
  one row per fix: `ts, latitude, longitude, speed, altitude`.
- **Climate → wide.** Same pattern, low frequency: `ts, inside_temp, outside_temp`.
- **Motor → long.** Its signals are sampled at different sub-second offsets, so a
  wide table would be mostly NULL. Long format `ts, signal, value, cell_index`
  stays faithful and lossless.

**Which data quality checks would you implement?**
Timestamp within a plausible window; GPS inside a Europe bounding box; speed
0–250 km/h; temperature −40–60 °C; RPM `32767` saturation-sentinel and negatives
rejected; and a guard against non-scalar signal values (e.g. `hvCellVoltages`,
which packs a per-cell list).

**How would you handle invalid records?**
Quarantine, not drop. Invalid rows go to a single `rejected` table tagged with
their source table and signal, so filtering is fully auditable.

*Output: clean, normalized time-series tables (GPS, Climate, Motor) + quarantine.*

### Exercise 3 — Gold Layer

**How would you resample GPS data to 1-minute buckets?**
Convert each fix to a datetime, `resample("1min")` per vehicle. Within a bucket
(~30 raw fixes): position → `last` (representative location at minute-end), speed
→ `mean` + `max`, altitude → `mean`, plus a `gps_samples` count for coverage.

**How would you handle missing temperature values?**
Temperature is a slow-changing level, so resample to 1-minute `mean` then
forward-fill short gaps (≤ 10 min). Beyond that the value is left NULL rather
than carried forward into staleness.

**Which aggregation strategy would you use for each signal?**
By physical type — *position* → last; *rates* (speed, rpm, charge power) → mean
(+ max for peaks); *levels* (SOC) → last; *slow levels* (temperature) → mean +
short forward-fill.

**How would you represent missing data?**
Explicit NULL, never 0 — because 0 is a real value (0 km/h = stopped, 0 kW = not
charging). The `gps_samples` count lets a consumer distinguish a sparse minute
from an empty one.

**Optional — implemented:**
- **Trip detection:** moving runs (`speed > 5 km/h`), with short stops (< 5 min)
  kept inside one trip; a trip ends on charging or 5+ parked minutes.
- **Charging detection:** charge power > 1 kW *while parked* — the parked guard
  excludes regenerative braking (which also pushes power in, but while moving).
- **Distance per trip:** haversine between consecutive minute positions, with a
  guard that rejects physically impossible GPS reacquisition jumps (> 250 km/h).

*Output: a 1-minute aggregated dataset (240,747 rows × 18 columns) ready for
analytics or ML.*

---

## Key design decisions

**Bronze — keep the payload sealed.** 

**Silver — table shape follows the data.** 

**Gold — aggregate by physical type.** Level vs rate vs position drives every
choice. Missing data stays NULL. Derived features handle EV-specific edge cases
(regen ≠ charging; GPS reacquisition jumps ≠ travel).

---

## Scope & what I'd add at scale

The pipeline is built for a static file. A few things were **consciously left
out** because the data didn't justify the complexity — but here's where they'd go:

- **Table format → Delta / Iceberg.** Replaces the hand-rolled pieces
  (idempotent rebuild, compaction, schema guards) with ACID, time travel,
  `MERGE` upserts, and native schema evolution for mixed firmware.
- **Incremental load.** The `device_id/event_date` layout is already
  incremental-ready — process only new partitions instead of full rebuilds.


---

## Repo layout

```
Data/            raw input + transformed output (gitignored)
src/
  config.py      central paths
  bronze.py      Layer 1
  silver.py      Layer 2  (+ utils.py validation)
  gold.py        Layer 3
main.py          runs all three layers in order
```
