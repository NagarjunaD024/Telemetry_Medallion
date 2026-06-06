"""
utils.py — data validation helpers for the Silver layer.

Each `validate_*` function returns:
    True   -> row is valid   (keep it)
    False  -> row is invalid (quarantine it)

Geographic bounds are scoped to Europe, since this is an EU vehicle fleet.
"""


TS_MIN, TS_MAX = 1_600_000_000, 1_700_000_000   # plausible Unix epoch window
LAT_MIN, LAT_MAX = 34.0, 72.0      # Europe
LON_MIN, LON_MAX = -11.0, 32.0     # Europe
SPEED_MIN, SPEED_MAX = 0.0, 250.0  # km/h; Autobahn-realistic ceiling
TEMP_MIN, TEMP_MAX   = -40.0, 60.0 # deg C; cabin/ambient plausibility
RPM_SENTINEL = 32767               # 2^15-1 saturation / error code

def is_scalar(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)

def valid_ts(ts):
    return is_scalar(ts) and TS_MIN <= ts <= TS_MAX

def in_range(value, lo, hi):
    return is_scalar(value) and lo <= value <= hi

def validate_gps(row):
    if not valid_ts(row["ts"]): return False
    if not in_range(row["latitude"], LAT_MIN, LAT_MAX): return False
    if not in_range(row["longitude"], LON_MIN, LON_MAX): return False
    if row["speed"] is not None and not in_range(row["speed"], SPEED_MIN, SPEED_MAX): return False
    return True

def validate_climate(row):
    if not valid_ts(row["ts"]): return False
    for field in ("inside_temp", "outside_temp"):
        v = row[field]
        if v is not None and not in_range(v, TEMP_MIN, TEMP_MAX): return False
    return True

def validate_motor(row):
    if not valid_ts(row["ts"]): return False
    if not is_scalar(row["value"]): return False
    if row["signal"] == "rpmEmotor" and (row["value"] == RPM_SENTINEL or row["value"] < 0): return False
    return True

def make_reject(device_id, ts, table, signal):
    """Uniform schema so all rejects fit one combined quarantine table."""
    return {"device_id": device_id, "ts": ts, "table": table, "signal": signal}