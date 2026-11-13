#!/usr/bin/env python3
"""Focus only on OLSON_ID - how to get it. No airport data, no input/ reads.
OLSON_ID = IANA zone name from coordinates via timezonefinder.
RAWOFFSET = standard offset via tzdata + zoneinfo (min of 12x15th).
Usage: .venv/bin/python demo/get_olson_id.py --lat 32.847 --lon -96.851
"""
import argparse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def get_olson_id(lat: float, lon: float, year: int = 2026):
    """Returns (OLSON_ID, RAWOFFSET_sec). No airport data needed."""
    from timezonefinder import TimezoneFinder
    tf = TimezoneFinder()  # loads polygon data once (~50MB)
    zone = tf.timezone_at(lat=lat, lng=lon)  # None if ocean
    if not zone:
        return None, None
    zi = ZoneInfo(zone)  # uses installed tzdata 2026f
    offs = []
    for m in range(1, 13):
        dt = datetime(year, m, 15, 12, 0, tzinfo=timezone.utc)
        o = dt.astimezone(zi).utcoffset()
        if o is not None:
            offs.append(int(o.total_seconds()))
    return zone, (min(offs) if offs else None)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--lat", type=float, default=32.847)
    ap.add_argument("--lon", type=float, default=-96.851)
    ap.add_argument("--year", type=int, default=2026)
    a = ap.parse_args()
    z, r = get_olson_id(a.lat, a.lon, a.year)
    print(f"lat={a.lat} lon={a.lon} -> OLSON_ID={z} RAWOFFSET={r}")
    # more samples, no airport data
    for la, lo in [(32.693, -100.95), (40.641, -73.778), (51.470, -0.454), (35.549, 139.78)]:
        print(f"{la},{lo} -> {get_olson_id(la, lo, a.year)}")
