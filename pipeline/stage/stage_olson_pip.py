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

    def run(self, batch_id: str | None = None) -> pd.DataFrame:
        from utils.common import GeoValidator, run_batch_id, timestamped_path
        batch_id = batch_id or run_batch_id()
        setup_logging()
        try:
            from timezonefinder import TimezoneFinder
        except ImportError as e:
            raise RuntimeError(f"timezonefinder lib required: {e}")
        # read latest timestamped enriched STG
        from utils.common import latest_stage_file, BASE_DIR
        try:
            cands = sorted((BASE_DIR / "output" / "stage").glob("LOC_AIRPORT_STG_*.csv"))
            cands = [p for p in cands if "jep_only" not in p.name]
            in_path = max(cands, key=lambda p: p.stat().st_mtime) if cands else INPUT_STG
        except (OSError, ValueError):
            in_path = INPUT_STG
        if not in_path.is_file():
            raise FileNotFoundError(f"Enriched STG missing (run BTS first): {in_path}")
        df_in = pd.read_csv(in_path, dtype=str, keep_default_na=False)
        for c in ("ICAO_ARPT_CDE", "FINAL_LAT", "FINAL_LON"):
            if c not in df_in.columns:
                raise ValueError(f"Enriched STG missing column: {c} (run BTS overwrite first)")

        tf = TimezoneFinder()
        errors = ErrorCapture(self.config, stage="olson_pip")
        # reuse olson templates for errors (same shape)
        ignores = _make_ignore_capture()
        batch = batch_id
        rows = []
        counts = {"rows_read": len(df_in), "loaded": 0, "failed": 0, "no_zone": 0, "bad_coords": 0}
        for _, r in df_in.iterrows():
            icao = (r.get("ICAO_ARPT_CDE", "") or "").strip()
            la_s, lo_s = (r.get("FINAL_LAT", "") or "").strip(), (r.get("FINAL_LON", "") or "").strip()
            la, lo, _ = GeoValidator.validate(la_s, lo_s)
            if la is None or lo is None:
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
        from utils.common import timestamped_path
        out_csv = timestamped_path(OUTPUT_CSV, batch_id)
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv, index=False)
        log.info("Wrote %d rows -> %s", len(df), out_csv)
        errors.save(APP.error_report, append=True)
        log.info(errors.summary("olson_pip"))
        ignores.save(APP.ignore_report, append=True)
        log.info("IGNORES stage=olson_pip count=%d", len(ignores))
        try:
            df.attrs["counts"] = counts
            df.attrs["output"] = str(out_csv)
            df.attrs["batch_id"] = batch_id
        except (AttributeError, ValueError, TypeError):
            pass
        return df


def run(batch_id: str | None = None) -> pd.DataFrame:
    return OlsonPipLoader().run(batch_id=batch_id)


def metadata_rows(result, run_ts: str) -> list[dict]:
    from utils.summary import StageSummary
    try:
        attrs = getattr(result, "attrs", {}) or {}
        counts = dict(attrs.get("counts", {}))
        out = attrs.get("output", str(OUTPUT_CSV))
    except (AttributeError, ValueError, TypeError):
        counts, out = {}, str(OUTPUT_CSV)
    if result is None and not counts:
        return []
    return [{"run_timestamp": run_ts, "status": "COMPLETED", "stage": "olson_pip", "tag": "olson_pip",
             "summary": StageSummary.olson_pip(counts),
             "output": str(out),
             "error_report": str(APP.error_report), "ignore_report": str(APP.ignore_report)}]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    df = run()
    print(f"Olson PIP done: rows={len(df)} -> {OUTPUT_CSV}")
