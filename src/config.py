"""
config.py — central paths and layer locations for the telemetry lakehouse.

All scripts import from here so paths are defined once.
"""
from pathlib import Path

# Repo root = two levels up from this file (src/config.py -> repo root)
ROOT = Path(__file__).resolve().parent.parent

# Data layers
DATA_DIR    = ROOT / "Data"
RAW_PATH    = DATA_DIR / "raw" / "test_data.json"
BRONZE_PATH = DATA_DIR / "Transformed" / "bronze"
SILVER_PATH = DATA_DIR / "Transformed" / "silver"
GOLD_PATH   = DATA_DIR / "Transformed" / "gold"