import json
from collections import defaultdict
from datetime import datetime, timezone
from config import RAW_PATH


device_docs   = defaultdict(int)
device_ts_min = {}
device_ts_max = {}

with open(RAW_PATH) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        doc = json.loads(line)
        did = doc["metaObd"]["id"]
        ts  = doc["metaObd"]["timestamp"]

        device_docs[did] += 1
        if did not in device_ts_min or ts < device_ts_min[did]:
            device_ts_min[did] = ts
        if did not in device_ts_max or ts > device_ts_max[did]:
            device_ts_max[did] = ts

# ── Summary ──────────────────────────────────────────────
total_docs    = sum(device_docs.values())
unique_devices = len(device_docs)

print(f"Total documents : {total_docs:,}")
print(f"Unique devices  : {unique_devices}")
print()

if unique_devices == 1:
    print("Verdict: PRIVATE dataset — single vehicle")
elif unique_devices <= 5:
    print("Verdict: SMALL FLEET — likely private/prosumer")
else:
    print("Verdict: FLEET dataset — multiple vehicles")

print()
print(f"{'Device ID':<25} {'Docs':>8}  {'First Seen':<22} {'Last Seen':<22} {'Active Days':>10}")
print("-" * 90)

for did, count in sorted(device_docs.items(), key=lambda x: -x[1]):
    first = datetime.fromtimestamp(device_ts_min[did], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    last  = datetime.fromtimestamp(device_ts_max[did], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    days  = (device_ts_max[did] - device_ts_min[did]) / 86400
    print(f"{did:<25} {count:>8,}  {first:<22} {last:<22} {days:>10.1f}")