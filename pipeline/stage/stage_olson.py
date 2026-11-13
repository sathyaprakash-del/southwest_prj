"""Stage Olson csv direct source -> LOC_OLSON_ID_STG (class-based, utils shared).

Independent load from input/loc_olson_id_stg_export.csv (Google export).
No Jeppesen join filter, audit vs Jeppesen ICAO only.
Errors -> central ERROR_REPORT.csv, ignores -> central IGNORE_REPORT.csv.
No separate olson_errors.csv.
"""
import logging
import re
import sys
from pathlib import Path

import pandas as pd

try:
    from utils.common import APP, setup_logging as _setup_logging
    from utils.error_capture import ErrorCapture
except ImportError:  # direct script run: python pipeline/stage_olson.py
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from utils.common import APP, setup_logging as _setup_logging
    from utils.error_capture import ErrorCapture

log = logging.getLogger(__name__)

_CONFIG = APP.config
_OLSON = _CONFIG.get("olson", {})

OUTPUT_CSV = APP.olson_output
OLSON_CSV = APP.olson_csv
LOG_FILE = APP.olson_log


def setup_logging() -> Path:
    return _setup_logging(__name__, LOG_FILE)


def _make_ignore_capture(stage="olson") -> ErrorCapture:
    cap = ErrorCapture(_CONFIG, stage=stage)
    ign = _CONFIG.get("ignores", {})
    if ign.get("templates"):
        cap.templates = ign["templates"]
    return cap


class OlsonLoader:
    """Class wrapper around Olson csv load (uses utils)."""

    def __init__(self, config: dict | None = None):
        self.config = config or _CONFIG
        self.olson = self.config.get("olson", _OLSON)

    def run(self, batch_id: str | None = None) -> pd.DataFrame:
        from utils.common import run_batch_id, timestamped_path
        batch_id = batch_id or run_batch_id()
        setup_logging()
        oc = self.olson
        cols = list(oc.get("columns", ["BATCHID", "ICAO_CDE", "OLSON_ID", "RAWOFFSET"]))
        min_len = int(oc.get("icao_min_length", 3))
        max_len = int(oc.get("icao_max_length", 4))
        olson_re = re.compile(oc.get("olson_id_pattern", r"^[A-Za-z_]+(/[A-Za-z0-9_+\-]+)*$"))
        raw_re = re.compile(oc.get("rawoffset_pattern", r"^-?\d+$"))
        batch_mode = oc.get("batch_id_mode", "run")
        run_batch = batch_id

        if not OLSON_CSV.is_file():
            raise FileNotFoundError(f"Olson source missing: {OLSON_CSV}")

        enc = APP.olson_encoding
        delim = APP.olson_delimiter
        log.info("Loading Olson %s", OLSON_CSV)
        df_raw = pd.read_csv(OLSON_CSV, dtype=str, keep_default_na=False,
                             encoding=enc, sep=delim)
        # normalize headers (strip quotes/spaces already handled by pandas)
        df_raw.columns = [c.strip() for c in df_raw.columns]

        errors = ErrorCapture(self.config, stage="olson")
        ignores = _make_ignore_capture("olson")

        seen: dict[str, dict] = {}
        rows: list[dict] = []
        counts = {"rows_read": 0, "loaded": 0, "failed": 0, "duplicates": 0,
                  "batch_id_differs": 0, "in_jeppesen": 0, "not_in_jeppesen": 0,
                  "jeppesen_without_olson": 0}

        for _, r in df_raw.iterrows():
            counts["rows_read"] += 1
            icao = str(r.get("ICAO_CDE", "")).strip()
            oid = str(r.get("OLSON_ID", "")).strip()
            raw = str(r.get("RAWOFFSET", "")).strip()
            file_batch = str(r.get("BATCHID", "")).strip()
            data = "|".join(str(r.get(c, "")) for c in df_raw.columns)
            err = None
            err_tpl = ""
            if not (min_len <= len(icao) <= max_len):
                err = f"ICAO_CDE length {len(icao)} not in [{min_len}, {max_len}]"
                err_tpl = "olson_icao"
            elif not olson_re.match(oid):
                err = f"OLSON_ID '{oid}' is blank or not in Area/City form"
                err_tpl = "olson_id"
            elif not raw_re.match(raw):
                err = f"RAWOFFSET '{raw}' is not a whole number of seconds"
                err_tpl = "olson_raw"
            elif icao in seen:
                err = "duplicate ICAO_CDE (first row kept)"
                err_tpl = "olson_dup"
            if err:
                counts["failed"] += 1
                if icao in seen:
                    counts["duplicates"] += 1
                    ignores.add("olson_dup", icao, data, tag="olson")
                else:
                    errors.add(err_tpl, icao, data, tag="olson")
                continue
            batch = run_batch if batch_mode == "run" else (file_batch or run_batch)
            if file_batch and file_batch != batch:
                counts["batch_id_differs"] += 1
                ignores.add("olson_batch_diff", icao, data, tag="olson")
            seen[icao] = {"olson_id": oid, "raw": raw}
            rows.append({"BATCHID": batch, "ICAO_CDE": icao, "OLSON_ID": oid, "RAWOFFSET": raw})
            counts["loaded"] += 1

        # audit vs Jeppesen ICAO (no filter, audit only) - latest timestamped STG
        try:
            from utils.common import BASE_DIR
            cands = sorted((BASE_DIR / "output" / "stage").glob("LOC_AIRPORT_STG_*.csv"))
            cands = [p for p in cands if "jep_only" not in p.name]
            jep_path = max(cands, key=lambda p: p.stat().st_mtime) if cands else APP.airport_stg
            if jep_path.is_file():
                jep_df = pd.read_csv(jep_path, dtype=str, keep_default_na=False, usecols=["ICAO_ARPT_CDE"])
                jep_icao = set(jep_df["ICAO_ARPT_CDE"].str.strip())
                olson_icao = set(seen.keys())
                counts["in_jeppesen"] = len(olson_icao & jep_icao)
                counts["not_in_jeppesen"] = len(olson_icao - jep_icao)
                counts["jeppesen_without_olson"] = len(jep_icao - olson_icao)
                for icao in sorted(olson_icao - jep_icao):
                    v = seen[icao]
                    ignores.add("olson_no_jep", icao, f"{icao}|{v['olson_id']}|{v['raw']}", tag="olson")
            else:
                log.warning("Jeppesen STG not found for Olson audit: %s", jep_path)
        except (KeyError, ValueError, TypeError, OSError) as e:
            log.warning("Olson Jeppesen audit failed: %s", e)

        df = pd.DataFrame(rows, columns=cols)
        if not df.empty:
            df = df.astype(str)
        out_csv = timestamped_path(OUTPUT_CSV, batch_id)
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv, index=False)
        log.info("Wrote %d rows -> %s", len(df), out_csv)

        errors.save(APP.error_report, append=True)
        log.info(errors.summary("olson"))
        ignores.save(APP.ignore_report, append=True)
        log.info("IGNORES stage=olson count=%d", len(ignores))

        try:
            df.attrs["counts"] = counts
            df.attrs["output"] = str(out_csv)
            df.attrs["batch_id"] = batch_id
        except (AttributeError, ValueError, TypeError) as e:
            log.warning("Could not attach olson counts: %s", e)
        return df


def run(batch_id: str | None = None) -> pd.DataFrame:
    return OlsonLoader().run(batch_id=batch_id)


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
    return [{"run_timestamp": run_ts, "status": "COMPLETED", "stage": "olson", "tag": "olson",
             "summary": StageSummary.olson(counts),
             "output": str(out),
             "error_report": str(APP.error_report), "ignore_report": str(APP.ignore_report)}]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    df = run()
    print(f"Olson done: rows={len(df)} -> {OUTPUT_CSV}")
