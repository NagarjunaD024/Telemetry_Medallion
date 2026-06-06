"""Bronze Layer — land raw telemetry as partitioned Parquet."""
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds

from config import RAW_PATH, BRONZE_PATH

CHUNK_SIZE  = 50_000   # docs held in memory per write, better memory handling for large datasets


def read_chunks(path, size):
    """Yield lists of raw JSON lines, `size` at a time."""
    batch = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                batch.append(line)
            if len(batch) >= size:
                yield batch
                batch = []
    if batch:
        yield batch


def to_table(lines, ingested_at):
    """Extract partition/identifier fields; keep the raw doc intact."""
    rows = []
    for line in lines:
        meta = json.loads(line)["metaObd"]
        ts = int(meta["timestamp"])
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        rows.append({
            "device_id":    meta["id"].replace(":", ""),   # partition key
            "batch_ts":     ts,                             # send time (Unix)
            "event_date":   dt.strftime("%Y-%m-%d"),        # partition key
            "ingestion_ts": ingested_at,                    # lineage
            "raw_payload":  line,                           # untouched signals
        })
    return pa.Table.from_pylist(rows)


def main():
    if BRONZE_PATH.exists():
        shutil.rmtree(BRONZE_PATH)          # idempotent rebuild
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    ingested_at = datetime.now(timezone.utc)
    total = 0

    for i, lines in enumerate(read_chunks(RAW_PATH, CHUNK_SIZE)):
        table = to_table(lines, ingested_at)
        ds.write_dataset(
            table,
            base_dir=BRONZE_PATH,
            format="parquet",
            partitioning=["device_id", "event_date"],   # vehicle, then time
            partitioning_flavor="hive",
            existing_data_behavior="overwrite_or_ignore",
            basename_template=f"part-{i}-{{i}}.parquet",
        )
        total += table.num_rows
        print(f"chunk {i}: {table.num_rows:,} rows (total {total:,})")

    print(f"Done — {total:,} docs written to {BRONZE_PATH}")


if __name__ == "__main__":
    main()