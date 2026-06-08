# main.py  (repo root)
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent / "src"))   # make src/ importable

from bronze import main as build_bronze
from silver import main as build_silver
from gold import main as build_gold

if __name__ == "__main__":
    build_bronze()
    build_silver()
    build_gold()
    print("\nPipeline completed successfully!")