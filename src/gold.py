"""
Gold Layer — analytics-ready dataset aligned to 1-minute intervals.

Reads the three Silver tables (gps / climate / motor), resamples each per
vehicle to fixed 1-minute buckets, joins them into one wide table, and derives
trip + charging features. Output: one row per (device_id, minute).

Aggregation strategy (level vs rate vs position):
    position (lat/lon) -> last      | speed -> mean + max
    altitude           -> mean      | temperature -> mean + short ffill
    rpm                -> mean + max | soc (level) -> last
    charge_power       -> mean
Missing data is kept NULL (never 0). gps_samples records per-bucket coverage.
"""
import math
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

from config import SILVER_PATH, GOLD_PATH

# --- tunable parameters ---
FREQ = "1min"
MOVING_KMH = 5.0          # above this = moving (ignores parked GPS jitter)
TRIP_GAP_MIN = 5          # parked gaps shorter than this stay within one trip
CHARGING_KW = 1.0         # charge power above this = charging
TEMP_FFILL_LIMIT = 10     # carry temperature forward up to 10 minutes
MAX_KMH = 250.0          # reject GPS jumps that imply faster than this (treat as 0 distance)


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def load_device(table, device_id, columns=None):
    """Read one vehicle's slice of a Silver table, with a datetime index column."""
    dataset = ds.dataset(SILVER_PATH / f"{table}.parquet", format="parquet")
    df = dataset.to_table(filter=ds.field("device_id") == device_id,
                          columns=columns).to_pandas()
    if not df.empty:
        df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    return df


# ---------------------------------------------------------------------------
# Resample each domain to 1-minute buckets
# ---------------------------------------------------------------------------
def resample_gps(gps):
    g = gps.set_index("dt").sort_index()
    return g.resample(FREQ).agg(
        lat=("latitude", "last"),          # position -> last fix in the minute
        lon=("longitude", "last"),
        speed_avg=("speed", "mean"),       # rate -> average pace
        speed_max=("speed", "max"),        #         and peak
        altitude=("altitude", "mean"),
        gps_samples=("latitude", "count"), # coverage: how many fixes backed this row
    )


def resample_climate(clim):
    c = clim.set_index("dt").sort_index()
    out = c.resample(FREQ).agg(
        inside_temp=("inside_temp", "mean"),
        outside_temp=("outside_temp", "mean"),
    )
    # temperature is a slow physical level -> carry forward across short gaps
    out = out.ffill(limit=TEMP_FFILL_LIMIT)
    return out


def resample_motor(motor):
    """Motor is long format -> resample each representative signal separately."""
    def series(signal):
        s = motor[motor.signal == signal].set_index("dt")["value"].sort_index()
        return s if not s.empty else None

    parts = {}
    rpm = series("rpmEmotor")
    if rpm is not None:
        r = rpm.resample(FREQ)
        parts["rpm_avg"] = r.mean()        # rate -> mean + max
        parts["rpm_max"] = r.max()
    soc = series("hvSocActualBms")
    if soc is not None:
        parts["soc"] = soc.resample(FREQ).last()        # level -> last
    chg = series("hvChargePower")
    if chg is not None:
        parts["charge_power_avg"] = chg.resample(FREQ).mean()  # rate -> mean

    return pd.DataFrame(parts)


# ---------------------------------------------------------------------------
# Derived features: moving / charging / trips / distance
# ---------------------------------------------------------------------------
def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def add_features(df):
    """Add is_moving, is_charging, trip_id, and per-minute distance_km."""
    df = df.copy()

    # Columns may be absent if a vehicle had no readings for that signal -> treat as 0.
    speed = df["speed_avg"] if "speed_avg" in df else pd.Series(0.0, index=df.index)
    power = df["charge_power_avg"] if "charge_power_avg" in df else pd.Series(0.0, index=df.index)

    df["is_moving"] = speed.fillna(0) > MOVING_KMH
    # Charging = power IN while parked (the parked guard excludes regen braking).
    df["is_charging"] = (power.fillna(0) > CHARGING_KW) & (speed.fillna(0) < MOVING_KMH)

    # Trip id: new trip when moving after being parked; ends on charging or 5 parked min.
    trip_id, parked_run, in_trip, ids = 0, 0, False, []
    for moving, charging in zip(df["is_moving"], df["is_charging"]):
        if charging:
            in_trip, parked_run = False, 0
            ids.append(pd.NA)
        elif moving:
            if not in_trip:
                trip_id += 1
                in_trip = True
            parked_run = 0
            ids.append(trip_id)
        else:
            parked_run += 1
            if in_trip and parked_run >= TRIP_GAP_MIN:
                in_trip = False
            ids.append(trip_id if in_trip else pd.NA)
    df["trip_id"] = ids

    # Distance: haversine from previous position within a trip, with GPS-jump guard.
    if "lat" not in df:                      # vehicle had no GPS at all
        df["distance_km"] = 0.0
        return df
    dist, prev = [0.0] * len(df), None
    for i, (lat, lon, tid, minute) in enumerate(df[["lat", "lon", "trip_id", "minute"]].to_records(index=False)):
        same_trip = prev is not None and pd.notna(tid) and pd.notna(prev[2]) and prev[2] == tid
        if same_trip and pd.notna(lat) and pd.notna(lon) and pd.notna(prev[0]) and pd.notna(prev[1]):
            d = haversine_km(prev[0], prev[1], lat, lon)
            gap_min = (pd.Timestamp(minute) - pd.Timestamp(prev[3])) / pd.Timedelta(minutes=1)
            implied_kmh = d / (gap_min / 60) if gap_min > 0 else 0
            dist[i] = 0.0 if implied_kmh > MAX_KMH else d
        prev = (lat, lon, tid, minute)
    df["distance_km"] = dist
    return df


# ---------------------------------------------------------------------------
# Build one vehicle's gold table
# ---------------------------------------------------------------------------
def build_device(device_id):
    gps = load_device("gps", device_id)
    clim = load_device("climate", device_id)
    motor = load_device("motor", device_id, columns=["ts", "signal", "value", "device_id"])

    frames = []
    if not gps.empty:
        frames.append(resample_gps(gps))
    if not clim.empty:
        frames.append(resample_climate(clim))
    if not motor.empty:
        m = resample_motor(motor)
        if not m.empty:
            frames.append(m)
    if not frames:
        return pd.DataFrame()

    # outer-join all domains on the shared 1-minute index
    gold = pd.concat(frames, axis=1, sort=True)
    # drop minutes that have no signal at all (vehicle off / no data)
    value_cols = [c for c in gold.columns if c != "gps_samples"]
    gold = gold.dropna(how="all", subset=value_cols)

    gold["minute"] = gold.index          # add BEFORE add_features (distance loop needs it)
    gold = add_features(gold)
    gold["device_id"] = device_id
    gold = gold.reset_index(drop=True)

    # put the keys first for readability
    keys = ["device_id", "minute"]
    cols = keys + [c for c in gold.columns if c not in keys]
    return gold[cols]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def main():
    GOLD_PATH.mkdir(parents=True, exist_ok=True)

    devices = (ds.dataset(SILVER_PATH / "gps.parquet", format="parquet")
                 .to_table(columns=["device_id"])
                 .column("device_id").unique().to_pylist())

    parts, total = [], 0
    for dev in sorted(devices):
        g = build_device(dev)
        if not g.empty:
            parts.append(g)
            total += len(g)
            print(f"  {dev}: {len(g):,} minute-rows, trips={g['trip_id'].dropna().nunique()}")

    # Gold is small and read whole (ML/analytics) -> single compacted file
    out = GOLD_PATH / "telemetry_1min.parquet"
    pd.concat(parts, ignore_index=True).to_parquet(out, index=False)
    print(f"\nGold complete: {total:,} rows -> {out}")


if __name__ == "__main__":
    main()