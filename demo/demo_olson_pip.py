"""Demo olson_pip - pip-only OLSON_ID from enriched LOC_AIRPORT_STG.csv.
No input/loc_olson csv read. Uses FINAL_LAT/LON -> timezonefinder -> tzdata.
Run: .venv/bin/python demo/demo_olson_pip.py [--limit 5]
"""
import argparse
import csv
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
STG = BASE / "output" / "stage" / "LOC_AIRPORT_STG.csv"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=5)
    a = ap.parse_args()
    from timezonefinder import TimezoneFinder
    tf = TimezoneFinder()
    print(f"[DEMO] Reading enriched {STG} (BTS-overwritten STG itself)")
    rows = list(csv.DictReader(open(STG, encoding="utf-8")))
    print(f"[DEMO] STG rows={len(rows)} cols={len(rows[0]) if rows else 0}")
    for r in rows[:a.limit]:
        icao, la, lo = r["ICAO_ARPT_CDE"], r["FINAL_LAT"], r["FINAL_LON"]
        try:
            zone = tf.timezone_at(lat=float(la), lng=float(lo))
        except Exception as e:
            zone = f"ERR {e}"
        print(f"  {icao} FINAL {la},{lo} -> OLSON_ID={zone} (pip timezonefinder, no input csv)")
    print("[DEMO] Full pip stage: pipeline/stage_olson_pip.py -> stage/LOC_OLSON_ID_PIP_STG.csv")
    print("[DEMO] Ignores -> output/IGNORE_REPORT.csv (same 5-col header, no BATCH_TYPE)")


if __name__ == "__main__":
    main()
