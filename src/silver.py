"""
Silver Layer — explode raw Bronze docs into clean, normalized time-series tables,
then compact each table into a single Parquet file.

GPS / Climate -> WIDE (signals are timestamp-aligned)
Motor         -> LONG (signals sample at different sub-second offsets)
Invalid rows  -> one combined `rejected` table.

Pipeline:
    1. stream Bronze in batches -> write per-batch part files (memory-safe)
    2. compact each table's part files -> single <table>.parquet (device_id kept)
"""
import json
import shutil

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from config import BRONZE_PATH, SILVER_PATH
from utils import validate_gps, validate_climate, validate_motor, make_reject

TABLES = ["gps", "climate", "motor", "rejected"]


# ---------------------------------------------------------------------------
# 1. EXPLODE
# ---------------------------------------------------------------------------
def explode_doc(payload):
    doc = json.loads(payload)
    dev = doc["metaObd"]["id"].replace(":", "")
    gps, climate, motor, rejects = [], [], [], []

    drive = doc.get("driveState")
    if drive:
        bucket = {}
        for sig in ("latitude", "longitude", "speed", "altitude"):
            for r in drive.get(sig, []):
                bucket.setdefault(r[-1], {})[sig] = r[0]
        for ts, vals in bucket.items():
            row = {"device_id": dev, "ts": ts,
                   "latitude": vals.get("latitude"), "longitude": vals.get("longitude"),
                   "speed": vals.get("speed"), "altitude": vals.get("altitude")}
            gps.append(row) if validate_gps(row) else rejects.append(make_reject(dev, ts, "gps", "driveState"))

    clim = doc.get("climate")
    if clim:
        bucket = {}
        for sig in ("insideActualTemp", "outsideAirTemp"):
            for r in clim.get(sig, []):
                bucket.setdefault(r[-1], {})[sig] = r[0]
        for ts, vals in bucket.items():
            row = {"device_id": dev, "ts": ts,
                   "inside_temp": vals.get("insideActualTemp"),
                   "outside_temp": vals.get("outsideAirTemp")}
            climate.append(row) if validate_climate(row) else rejects.append(make_reject(dev, ts, "climate", "climate"))

    emotor = doc.get("eMotor")
    if emotor:
        for sig, arr in emotor.items():
            for r in arr:
                ts, val = r[-1], r[0]
                cell = r[1] if sig in ("hvCellVoltageMax", "hvCellVoltageMin") and len(r) == 3 else None
                row = {"device_id": dev, "ts": ts, "signal": sig, "value": val, "cell_index": cell}
                if validate_motor(row):
                    row["value"] = float(val)
                    motor.append(row)
                else:
                    rejects.append(make_reject(dev, ts, "motor", sig))

    return gps, climate, motor, rejects


def write_batch(table_name, rows, part):
    if not rows:
        return 0
    table = pa.Table.from_pylist(rows)
    ds.write_dataset(table, base_dir=SILVER_PATH / table_name, format="parquet",
                     partitioning=["device_id"], partitioning_flavor="hive",
                     existing_data_behavior="overwrite_or_ignore",
                     basename_template=f"part-{part}-{{i}}.parquet")
    return len(rows)


# ---------------------------------------------------------------------------
# 2. COMPACT
# ---------------------------------------------------------------------------
def unify_schema(schema):
    """Promote int columns that may also hold floats to float64."""
    promote = {"inside_temp", "outside_temp", "value", "latitude", "longitude",
               "speed", "altitude", "cell_index"}
    fields = [pa.field(f.name, pa.float64()) if f.name in promote and pa.types.is_integer(f.type) else f
              for f in schema]
    return pa.schema(fields)


def compact(table_name):
    src_dir = SILVER_PATH / table_name
    if not src_dir.exists():
        return
    unified = unify_schema(ds.dataset(src_dir, partitioning="hive").schema)
    dataset = ds.dataset(src_dir, partitioning="hive", schema=unified)
    out_file = SILVER_PATH / f"{table_name}.parquet"

    rows, writer = 0, None
    for batch in dataset.to_batches():
        if batch.num_rows == 0:
            continue
        table = pa.Table.from_batches([batch]).cast(unified, safe=False)
        if writer is None:
            writer = pq.ParquetWriter(out_file, unified)
        writer.write_table(table)
        rows += table.num_rows
    if writer:
        writer.close()
    shutil.rmtree(src_dir)            # drop the part-file folder
    print(f"  {table_name}: {rows:,} rows -> {out_file.name}")


# ---------------------------------------------------------------------------
# PIPELINE
# ---------------------------------------------------------------------------
def main():
    if SILVER_PATH.exists():
        shutil.rmtree(SILVER_PATH)

    bronze = ds.dataset(BRONZE_PATH, partitioning="hive")

    print("Exploding Bronze -> Silver part files...")
    for i, batch in enumerate(bronze.to_batches(columns=["raw_payload"])):
        gps, climate, motor, rejects = [], [], [], []
        for payload in batch.column("raw_payload").to_pylist():
            g, c, m, rj = explode_doc(payload)
            gps += g; climate += c; motor += m; rejects += rj
        write_batch("gps", gps, i)
        write_batch("climate", climate, i)
        write_batch("motor", motor, i)
        write_batch("rejected", rejects, i)

    print("Compacting Silver tables:")
    for t in TABLES:
        compact(t)

    print("Silver complete.")


if __name__ == "__main__":
    main()