import logging
import sys
from pathlib import Path

"""Stage BTS enrichment (class-based, utils shared). Missing stays inside BTS STG."""
import pandas as pd

try:
    from utils.common import APP, Constants, setup_logging as _setup_logging
    from utils.error_capture import ErrorCapture
except ImportError:  # direct script run: python pipeline/stage_bts.py
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from utils.common import APP, Constants, setup_logging as _setup_logging
    from utils.error_capture import ErrorCapture

log = logging.getLogger(__name__)

_CONFIG = APP.config
BTS_CSV = APP.bts_csv
AIRPORT_STG = APP.airport_stg
OUTPUT_CSV = APP.bts_output
LOG_FILE = APP.bts_log

BTS_COLUMNS = _CONFIG["bts"]["columns"]
COLUMN_PREFIX = _CONFIG["bts"]["column_prefix"]
LEFT_KEY = _CONFIG["joins"]["airport_code"]["jeppesen_column"]
RIGHT_KEY = _CONFIG["joins"]["airport_code"]["bts_field"]


def apply_accept_rules(df: pd.DataFrame, rules) -> pd.DataFrame:
    mask = pd.Series(True, index=df.index)
    for r in rules or []:
        field = r.get("field")
        if not field or field not in df.columns:
            continue
        if "equals" in r:
            mask &= df[field].fillna("") == r["equals"]
        elif r.get("is_empty"):
            mask &= df[field].fillna("") == ""
    return df[mask]


def setup_logging() -> Path:
    return _setup_logging(__name__, LOG_FILE)


def _make_ignore_capture(config=None) -> ErrorCapture:
    cap = ErrorCapture(config or _CONFIG, stage="bts")
    ign = (config or _CONFIG).get("ignores", {})
    if ign.get("templates"):
        cap.templates = ign["templates"]
    return cap


def collect_bts_ignores(df_airport: pd.DataFrame | None = None) -> ErrorCapture:
    """Build ignore capture for accept-rule rejects + dups + BTS with no Jeppesen."""
    ignores = _make_ignore_capture()
    try:
        src = _CONFIG.get("sources", {}).get("bts", {})
        df_full = pd.read_csv(BTS_CSV, dtype=str, keep_default_na=False,
                              encoding=src.get("encoding", "utf-8"), sep=src.get("delimiter", ","))
    except (OSError, ValueError) as e:
        log.warning("BTS ignore collect skipped: %s", e)
        return ignores
    rules = _CONFIG["bts"].get("accept_rules", [])
    # accept-rule rejects
    for _, r in df_full.iterrows():
        fail_latest = (r.get("AIRPORT_IS_LATEST", "") != "1")
        fail_thru = (r.get("AIRPORT_THRU_DATE", "") != "")
        if fail_latest or fail_thru:
            tpl = "bts_not_latest" if fail_latest else "bts_thru_date"
            ignores.add(tpl, r.get(RIGHT_KEY, ""), "|".join(str(r.get(c, "")) for c in BTS_COLUMNS), tag="bts")
    # duplicates (keep first)
    try:
        dups = df_full[df_full.duplicated(subset=[RIGHT_KEY], keep="first")]
        for _, r in dups.iterrows():
            ignores.add("bts_dup", r.get(RIGHT_KEY, ""), "|".join(str(r.get(c, "")) for c in BTS_COLUMNS), tag="bts")
    except (KeyError, ValueError) as e:
        log.warning("BTS dup ignore failed: %s", e)
    # BTS keys with no Jeppesen airport (silent drop in join)
    try:
        if df_airport is not None and RIGHT_KEY in df_full.columns and LEFT_KEY in df_airport.columns:
            jkeys = set(df_airport[LEFT_KEY].astype(str))
            # only latest-filtered BTS keys are joined
            df_f = apply_accept_rules(df_full, rules)
            df_f = df_f.drop_duplicates(subset=[RIGHT_KEY], keep="first")
            for _, r in df_f.iterrows():
                if r.get(RIGHT_KEY, "") not in jkeys:
                    bts_data = "|".join(str(r.get(c, "")) for c in BTS_COLUMNS)
                    ignores.add("bts_no_jep", r.get(RIGHT_KEY, ""), bts_data, tag="bts")
    except (KeyError, ValueError, AttributeError) as e:
        log.warning("BTS no-jep ignore failed: %s", e)
    return ignores


def load_bts(bts_csv: Path = BTS_CSV) -> pd.DataFrame:
    log.info("Loading BTS %s", bts_csv)
    src = _CONFIG.get("sources", {}).get("bts", {})
    df_full = pd.read_csv(bts_csv, dtype=str, keep_default_na=False,
                          encoding=src.get("encoding", "utf-8"), sep=src.get("delimiter", ","))
    missing = [c for c in BTS_COLUMNS if c not in df_full.columns]
    if missing:
        raise ValueError(f"BTS CSV missing columns: {missing}")
    n_before = len(df_full)
    df_full = apply_accept_rules(df_full, _CONFIG["bts"].get("accept_rules", []))
    log.info("BTS accept rules: %d -> %d rows (%d unique)",
             n_before, len(df_full), df_full[RIGHT_KEY].nunique() if RIGHT_KEY in df_full.columns else 0)
    if df_full.duplicated(subset=[RIGHT_KEY]).any():
        n_dup = int(df_full.duplicated(subset=[RIGHT_KEY]).sum())
        log.warning("BTS has %d duplicate AIRPORT keys; deduping", n_dup)
        df_full = df_full.sort_values(RIGHT_KEY, ascending=False)
        df_full = df_full.drop_duplicates(subset=[RIGHT_KEY], keep="first")
    df = df_full[BTS_COLUMNS].copy()
    df = df.rename(columns={c: f"{COLUMN_PREFIX}{c}" if c != RIGHT_KEY else c for c in df.columns})
    log.info("BTS rows=%d", len(df))
    return df


def load_airport_stg(airport_stg: Path = AIRPORT_STG) -> pd.DataFrame:
    from utils.common import latest_stage_file
    # timestamped outputs: pick latest LOC_AIRPORT_STG_*.csv, fallback fixed path
    latest = latest_stage_file("output/stage/LOC_AIRPORT_STG_*.csv")
    # exclude jep_only backups from candidate? latest_stage_file returns max mtime;
    # jep_only backup written just before overwrite in same run could be newer –
    # prefer non-backup files
    try:
        from pathlib import Path as _P
        from utils.common import BASE_DIR
        cands = sorted((BASE_DIR / "output" / "stage").glob("LOC_AIRPORT_STG_*.csv"))
        cands = [p for p in cands if "jep_only" not in p.name]
        if cands:
            latest = max(cands, key=lambda p: p.stat().st_mtime)
    except (OSError, ValueError):
        pass
    src = latest if latest is not None else airport_stg
    log.info("Loading Jeppesen staging %s", src)
    df = pd.read_csv(src, dtype=str, keep_default_na=False)
    if LEFT_KEY not in df.columns:
        raise ValueError(f"Airport STG missing join key: {LEFT_KEY}")
    log.info("Airport STG rows=%d", len(df))
    return df


def join_bts(df_airport: pd.DataFrame, df_bts: pd.DataFrame) -> pd.DataFrame:
    merged = df_airport.merge(df_bts, left_on=LEFT_KEY, right_on=RIGHT_KEY, how="left")
    if RIGHT_KEY in merged.columns:
        merged = merged.drop(columns=[RIGHT_KEY])
    if Constants.COL_BTS_LAT in merged.columns:
        matched = int(merged[Constants.COL_BTS_LAT].fillna("").ne("").sum())
    else:
        matched = 0
    log.info("Joined rows=%d matched=%d unmatched=%d", len(merged), matched, len(merged) - matched)
    return merged


def _jep_decimal_from_row(r, col: str) -> float | None:
    v = str(r.get(col, "")).strip()
    if v != "" and v.lower() != "nan":
        try:
            return float(v)
        except ValueError:
            return None
    return None


def resolve_final_coords(merged: pd.DataFrame, errors) -> pd.DataFrame:
    final_lat: list[str] = []
    final_lon: list[str] = []
    bts_cnt = 0
    fallback_cnt = 0
    fail_cnt = 0
    for _, r in merged.iterrows():
        key = r.get(Constants.COL_PRODUCT_ID, "") or r.get(LEFT_KEY, "")
        lat, lon, _ = errors.check(
            r.get(Constants.COL_BTS_LAT, ""), r.get(Constants.COL_BTS_LON, ""),
            source="BTS", record_id=key, tag="airport")
        if lat is not None and lon is not None:
            final_lat.append(str(lat))
            final_lon.append(str(lon))
            bts_cnt += 1
        else:
            jlat = _jep_decimal_from_row(r, Constants.COL_JEP_DEC_LAT)
            jlon = _jep_decimal_from_row(r, Constants.COL_JEP_DEC_LON)
            if jlat is not None and jlon is not None:
                final_lat.append(str(jlat))
                final_lon.append(str(jlon))
                fallback_cnt += 1
            else:
                final_lat.append("")
                final_lon.append("")
                fail_cnt += 1
    merged[Constants.COL_FINAL_LAT] = final_lat
    merged[Constants.COL_FINAL_LON] = final_lon
    log.info("FINAL coords: matched=%d fallback=%d fail=%d", bts_cnt, fallback_cnt, fail_cnt)
    try:
        merged.attrs["src_counts"] = {"matched": bts_cnt, "fallback": fallback_cnt, "fail": fail_cnt}
    except (AttributeError, ValueError, TypeError):
        pass
    return merged


def add_extra_columns(merged: pd.DataFrame) -> pd.DataFrame:
    conv: list[str] = []
    for _, r in merged.iterrows():
        lat_txt = str(r.get(Constants.COL_JEP_ARPT_LAT, "")).strip()
        lat_c = str(r.get(Constants.COL_JEP_COR_LAT, "")).strip()
        lat_d = str(r.get(Constants.COL_JEP_DEC_LAT, "")).strip()
        lon_txt = str(r.get(Constants.COL_JEP_ARPT_LONG, "")).strip()
        lon_c = str(r.get(Constants.COL_JEP_COR_LON, "")).strip()
        lon_d = str(r.get(Constants.COL_JEP_DEC_LON, "")).strip()
        conv.append(f"{lat_txt} [{lat_c}]->{lat_d}; {lon_txt} [{lon_c}]->{lon_d}")
    merged[Constants.COL_BTS_CONV] = conv
    try:
        from timezonefinder import TimezoneFinder
        tf = TimezoneFinder()
        tz_lib = True
    except Exception as e:
        log.warning("timezonefinder lib not available (%s); TIMEZONE will be empty but output continues", e)
        tf = None
        tz_lib = False
    tz_out: list[str] = []
    ok = 0
    for _, r in merged.iterrows():
        fl, fn = str(r.get(Constants.COL_FINAL_LAT, "")).strip(), str(r.get(Constants.COL_FINAL_LON, "")).strip()
        if tf is None or fl == "" or fn == "":
            tz_out.append("")
            continue
        try:
            tz = tf.timezone_at(lat=float(fl), lng=float(fn))
            tz_out.append(tz or "")
            if tz:
                ok += 1
        except Exception:
            tz_out.append("")
    merged[Constants.COL_TIMEZONE] = tz_out
    log.info("Extra cols: BTS_CONV done, TIMEZONE resolved=%d/%d (lib=%s, empty lat ok)", ok, len(merged), tz_lib)
    return merged


def log_missing_counts(df_airport: pd.DataFrame, bts_csv: Path = BTS_CSV) -> dict:
    df_full = pd.read_csv(bts_csv, dtype=str, keep_default_na=False)
    total = len(df_airport)

    def _miss(df_ded: pd.DataFrame) -> int:
        b = df_ded[BTS_COLUMNS].rename(
            columns={c: f"{COLUMN_PREFIX}{c}" if c != RIGHT_KEY else c for c in BTS_COLUMNS})
        m = df_airport.merge(
            b[[RIGHT_KEY, Constants.COL_BTS_LAT, Constants.COL_BTS_LON]],
            left_on=LEFT_KEY, right_on=RIGHT_KEY, how="left")
        lat_blank = m[Constants.COL_BTS_LAT].fillna("") == ""
        lon_blank = m[Constants.COL_BTS_LON].fillna("") == ""
        return int((lat_blank | lon_blank).sum())

    rules = _CONFIG["bts"].get("accept_rules", [])
    latest_col = next((r["field"] for r in rules if "equals" in r), RIGHT_KEY)
    if latest_col in df_full.columns:
        ded_all = df_full.sort_values(latest_col, ascending=False).drop_duplicates(RIGHT_KEY)
    else:
        ded_all = df_full.drop_duplicates(RIGHT_KEY)
    miss_without = _miss(ded_all)
    df_f = apply_accept_rules(df_full, rules)
    ded_with = df_f.drop_duplicates(RIGHT_KEY)
    miss_with = _miss(ded_with)

    log.info("COUNT WITHOUT filter - no filter added, exact values (ALL BTS %d unique): total=%d matched=%d missing=%d",
             ded_all[RIGHT_KEY].nunique(), total, total - miss_without, miss_without)
    log.info("COUNT WITH accept rules %s (%d unique): total=%d matched=%d missing=%d",
             rules, ded_with[RIGHT_KEY].nunique() if len(ded_with) else 0, total, total - miss_with, miss_with)
    log.warning("Missing BTS lat/lon WITHOUT=%d WITH=%d (diff=%d)", miss_without, miss_with, miss_with - miss_without)
    return {"without_filter": miss_without, "with_filter": miss_with, "total": total}


class BTSEnricher:
    """Class wrapper around BTS enrichment (uses utils)."""
    def __init__(self, config: dict | None = None):
        self.config = config or _CONFIG

    def run(self, batch_id: str | None = None) -> pd.DataFrame:
        from utils.common import run_batch_id, timestamped_path
        batch_id = batch_id or run_batch_id()
        setup_logging()
        df_bts = load_bts(BTS_CSV)
        df_airport = load_airport_stg(AIRPORT_STG)
        counts = log_missing_counts(df_airport, BTS_CSV)
        errors = ErrorCapture(self.config, stage="bts")
        ignores = collect_bts_ignores(df_airport)
        merged = join_bts(df_airport, df_bts)
        merged = resolve_final_coords(merged, errors)
        merged = add_extra_columns(merged)
        # BATCHID datetime YYYYMMDD_HHMM (no sec), same run batch_id
        merged["BATCHID"] = batch_id
        cols = ["BATCHID"] + [c for c in merged.columns if c != "BATCHID"]
        merged = merged[cols]
        out_csv = timestamped_path(OUTPUT_CSV, batch_id)
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        # keep Jeppesen-only backup (timestamped)
        try:
            backup = out_csv.parent / f"{OUTPUT_CSV.stem}.jep_only_{batch_id}.csv"
            df_airport.to_csv(backup, index=False)
            log.info("Backed up Jeppesen-only %d rows -> %s", len(df_airport), backup)
        except (OSError, ValueError) as e:
            log.warning("Jep-only backup failed: %s", e)
        merged.to_csv(out_csv, index=False)
        log.info("Wrote %d rows (%d cols) -> %s", len(merged), len(merged.columns), out_csv)
        errors.save(APP.error_report, append=True)
        log.info(errors.summary("bts"))
        ignores.save(APP.ignore_report, append=True)
        log.info("IGNORES stage=bts count=%d", len(ignores))
        counts["bts_errors"] = len(errors)
        try:
            _src = dict(getattr(merged, "attrs", {}).get("src_counts", {}))
        except (AttributeError, ValueError, TypeError):
            _src = {}
        n_missing = int(_src.get("fallback", 0))
        log.warning("Missing BTS lat/lon inside BTS STG (fallback): %d rows "
                    "(audit with/without=%s)", n_missing, counts)
        try:
            merged.attrs["counts"] = counts
            merged.attrs["output"] = str(out_csv)
            merged.attrs["batch_id"] = batch_id
        except (AttributeError, ValueError, TypeError) as e:
            log.warning("Could not attach audit counts to frame: %s", e)
        return merged


def run(batch_id: str | None = None) -> pd.DataFrame:
    return BTSEnricher().run(batch_id=batch_id)


def metadata_rows(result, run_ts: str) -> list[dict]:
    from utils.summary import StageSummary
    merged = result
    try:
        _src = dict(getattr(merged, "attrs", {}).get("src_counts", {}))
    except (AttributeError, ValueError, TypeError):
        _src = {}
    try:
        counts = dict(getattr(merged, "attrs", {}).get("counts", {}))
    except (AttributeError, ValueError, TypeError):
        counts = {}
    total = len(merged) if merged is not None else 0
    success_n = int(_src.get("fallback", 0))
    fail_n = int(_src.get("fail", 0))
    bts_errors = int(counts.get("bts_errors", 0))
    err, ign = "", str(APP.ignore_report)
    try:
        out = str(getattr(merged, "attrs", {}).get("output", str(APP.bts_output)))
    except (AttributeError, ValueError, TypeError):
        out = str(APP.bts_output)
    return [
        {"run_timestamp": run_ts, "status": "COMPLETED", "stage": "bts", "tag": "airport",
         "summary": StageSummary.bts_airport(total, bts_errors),
         "output": out, "error_report": err, "ignore_report": ign},
        {"run_timestamp": run_ts, "status": "COMPLETED", "stage": "bts", "tag": "non-latlon",
         "summary": StageSummary.bts_nonlatlon(counts, success_n, fail_n),
         "output": out, "error_report": str(APP.error_report), "ignore_report": ign},
    ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
