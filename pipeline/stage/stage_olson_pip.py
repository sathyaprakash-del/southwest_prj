"""Stage Olson PIP - pip-only Olson ID from enriched LOC_AIRPORT_STG.csv.

Reads FINAL_LAT/FINAL_LON per ICAO_ARPT_CDE (BTS-overwritten STG),
timezonefinder -> OLSON_ID, tzdata+ZoneInfo min-12x15th -> RAWOFFSET (sec, standard).
No input/loc_olson csv read. Independent.
Errors -> central ERROR_REPORT.csv, ignores -> IGNORE_REPORT.csv.
Output: stage/LOC_OLSON_ID_PIP_STG.csv [BATCHID,ICAO_CDE,OLSON_ID,RAWOFFSET]
"""
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from utils.common import APP, setup_logging as _setup_logging
    from utils.error_capture import ErrorCapture
except ImportError:  # direct run
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from utils.common import APP, setup_logging as _setup_logging
    from utils.error_capture import ErrorCapture

log = logging.getLogger(__name__)

_CONFIG = APP.config
INPUT_STG = APP.airport_stg  # enriched LOC_AIRPORT_STG.csv with FINAL_LAT/LON
OUTPUT_CSV = APP.olson_pip_output
LOG_FILE = APP.olson_pip_log

YEAR = 2026


def setup_logging() -> Path:
    return _setup_logging(__name__, LOG_FILE)


def _make_ignore_capture() -> ErrorCapture:
    cap = ErrorCapture(_CONFIG, stage="olson_pip")
    ign = _CONFIG.get("ignores", {})
    if ign.get("templates"):
        cap.templates = ign["templates"]
    return cap


def std_offset_seconds(olson_id: str, year: int = YEAR) -> int | None:
    from zoneinfo import ZoneInfo
    zi = ZoneInfo(olson_id)
    offs = []
    for m in range(1, 13):
        dt = datetime(year, m, 15, 12, 0, tzinfo=timezone.utc)
        o = dt.astimezone(zi).utcoffset()
        if o is not None:
            offs.append(int(o.total_seconds()))
    return min(offs) if offs else None


class OlsonPipLoader:
    def __init__(self, config: dict | None = None):
        self.config = config or _CONFIG

    def run(self) -> pd.DataFrame:
        setup_logging()
        try:
            from timezonefinder import TimezoneFinder
        except ImportError as e:
            raise RuntimeError(f"timezonefinder lib required: {e}")
        if not INPUT_STG.is_file():
            raise FileNotFoundError(f"Enriched STG missing (run BTS first): {INPUT_STG}")
        df_in = pd.read_csv(INPUT_STG, dtype=str, keep_default_na=False)
        for c in ("ICAO_ARPT_CDE", "FINAL_LAT", "FINAL_LON"):
            if c not in df_in.columns:
                raise ValueError(f"Enriched STG missing column: {c} (run BTS overwrite first)")

        tf = TimezoneFinder()
        errors = ErrorCapture(self.config, stage="olson_pip")
        # reuse olson templates for errors (same shape)
        ignores = _make_ignore_capture()
        batch = datetime.now().strftime("%Y%j%H%M%S")
        rows = []
        counts = {"rows_read": len(df_in), "loaded": 0, "failed": 0, "no_zone": 0, "bad_coords": 0}
        for _, r in df_in.iterrows():
            icao = (r.get("ICAO_ARPT_CDE", "") or "").strip()
            la_s, lo_s = (r.get("FINAL_LAT", "") or "").strip(), (r.get("FINAL_LON", "") or "").strip()
            try:
                la, lo = float(la_s), float(lo_s)
            except ValueError:
                la = lo = None
            if la is None or not (-90 <= la <= 90) or not (-180 <= lo <= 180):
                counts["failed"] += 1
                counts["bad_coords"] += 1
                errors.add("olson_icao", icao, f"{la_s}|{lo_s}", tag="olson_pip")
                ignores.add("olson_no_jep", icao, f"{la_s}|{lo_s}", tag="olson_pip")
                continue
            try:
                zone = tf.timezone_at(lat=la, lng=lo)
            except Exception as e:
                zone = None
                log.warning("tz lookup %s failed: %s", icao, e)
            if not zone:
                counts["failed"] += 1
                counts["no_zone"] += 1
                errors.add("olson_id", icao, f"{la}|{lo}", tag="olson_pip")
                continue
            try:
                raw = std_offset_seconds(zone, YEAR)
            except Exception as e:
                counts["failed"] += 1
                errors.add("olson_raw", icao, f"{zone}: {e}", tag="olson_pip")
                continue
            rows.append({"BATCHID": batch, "ICAO_CDE": icao, "OLSON_ID": zone, "RAWOFFSET": str(raw)})
            counts["loaded"] += 1

        df = pd.DataFrame(rows, columns=["BATCHID", "ICAO_CDE", "OLSON_ID", "RAWOFFSET"])
        if not df.empty:
            df = df.astype(str)
        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTPUT_CSV, index=False)
        log.info("Wrote %d rows -> %s", len(df), OUTPUT_CSV)
        errors.save(APP.error_report, append=True)
        log.info(errors.summary("olson_pip"))
        ignores.save(APP.ignore_report, append=True)
        log.info("IGNORES stage=olson_pip count=%d", len(ignores))
        try:
            df.attrs["counts"] = counts
        except (AttributeError, ValueError, TypeError):
            pass
        return df


def run() -> pd.DataFrame:
    return OlsonPipLoader().run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    df = run()
    print(f"Olson PIP done: rows={len(df)} -> {OUTPUT_CSV}")
