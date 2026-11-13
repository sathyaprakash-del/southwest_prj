"""Stage Innovata STN + TZ -> LOC_TMZN_STG (class-based, utils shared).

Port of test.py:stage_innovata, but errors go to the central
ERROR_REPORT.csv (APP.error_report) via ErrorCapture append.
No separate innovata_errors.csv is written.

Mapping (KT v16 s2.4):
  STN.STACODE (IATA) -> IATA_ARPT_CDE, STN.TZCODE -> TM_ZN_CDE,
  STN.CNTRYCODE -> CTRY_CDE, TZ.TZNAME -> TM_ZN_NME, etc.
  Join STN (CNTRYCODE,TZCODE) -> TZ (CNTRYCODE,TZCODE), 1:N periods.
"""
import logging
import re
import sys
from datetime import datetime, date, time
from pathlib import Path

import pandas as pd

try:
    from utils.common import APP, setup_logging as _setup_logging
    from utils.error_capture import ErrorCapture
except ImportError:  # direct script run: python pipeline/stage_innovata.py
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from utils.common import APP, setup_logging as _setup_logging
    from utils.error_capture import ErrorCapture

log = logging.getLogger(__name__)

_CONFIG = APP.config
_INNOVATA = _CONFIG.get("innovata", {})
_SOURCES = _CONFIG.get("sources", {})
_JOINS = _CONFIG.get("joins", {})


# Output: config has "stage/LOC_TMZN_STG.csv" (relative to output/)
def _resolve_output() -> Path:
    out_file = _INNOVATA.get("output_file", "stage/LOC_TMZN_STG.csv")
    p = Path(out_file)
    if p.is_absolute():
        return p
    # "stage/..." -> output/stage/..., bare name -> stage dir
    if str(out_file).startswith("stage/"):
        return APP.output_dir / out_file
    return APP.stage_dir / out_file.name


OUTPUT_CSV = _resolve_output()


def _resolve_log() -> Path:
    try:
        return APP.output_dir.parent / _CONFIG.get("logs", {}).get("innovata", "logs/stage_innovata.log")
    except (KeyError, AttributeError, TypeError):
        return APP.output_dir.parent / "logs" / "stage_innovata.log"


LOG_FILE = _resolve_log()


def setup_logging() -> Path:
    return _setup_logging(__name__, LOG_FILE)


def _normalize_delim(d: str | None, default: str = "\t") -> str:
    if d is None:
        return default
    if d in (r"\t", "\\t", "TAB", "tab"):
        return "\t"
    return d


def parse_dt(d: str, t: str) -> datetime:
    return datetime.strptime(f"{d}{t}", "%d%m%Y%H%M")


def gmt_minutes(v: str):
    """Innovata GMTVAR hours*100+minutes signed: '930' -> 570, '-300' -> -180."""
    try:
        n = int(str(v).strip())
    except (ValueError, AttributeError):
        return ""
    sign = -1 if n < 0 else 1
    n = abs(n)
    return sign * ((n // 100) * 60 + n % 100)


def innovata_lat(v: str):
    """172109S = 17 21 09 S -> decimal."""
    try:
        s = str(v).strip()
        if len(s) == 7 and s[6] in "NS":
            x = int(s[0:2]) + int(s[2:4]) / 60.0 + int(s[4:6]) / 3600.0
            return -x if s[6] == "S" else x
    except (ValueError, IndexError):
        pass
    return None


def innovata_lon(v: str):
    """1453034W = 145 30 34 W -> decimal."""
    try:
        s = str(v).strip()
        if len(s) == 8 and s[7] in "EW":
            x = int(s[0:3]) + int(s[3:5]) / 60.0 + int(s[5:7]) / 3600.0
            return -x if s[7] == "W" else x
    except (ValueError, IndexError):
        pass
    return None


def _station_key_field() -> str:
    return (
        _JOINS.get("airport_code", {}).get("innovata_station_field")
        or _CONFIG.get("joins", {}).get("airport_code", {}).get("innovata_station_field")
        or "STACODE"
    )


def _stn_tz_fields() -> tuple[list[str], list[str]]:
    j = _JOINS.get("station_to_timezone", {})
    st_fields = list(j.get("station_fields") or ["CNTRYCODE", "TZCODE"])
    tz_fields = list(j.get("timezone_fields") or ["CNTRYCODE", "TZCODE"])
    return st_fields, tz_fields


def load_tz_index(tz_path: Path, encoding: str, delimiter: str,
                  tz_fields: list[str]) -> dict:
    index: dict[tuple, list[dict]] = {}
    with tz_path.open("r", encoding=encoding, newline="") as fh:
        import csv
        for row in csv.DictReader(fh, delimiter=delimiter):
            try:
                row["_eff"] = parse_dt(row.get("EFFDATE", ""), row.get("EFFTIME", ""))
                row["_disc"] = parse_dt(row.get("DISCDATE", ""), row.get("DISCTIME", ""))
            except (ValueError, KeyError, TypeError):
                row["_eff"] = row["_disc"] = None
            index.setdefault(tuple((row.get(f, "") or "").strip() for f in tz_fields), []).append(row)
    for rows in index.values():
        rows.sort(key=lambda r: r.get("_eff") or datetime.min)
    return index


class InnovataParser:
    """Class wrapper around Innovata enrichment (uses utils)."""

    def __init__(self, config: dict | None = None):
        self.config = config or _CONFIG
        self.innovata = self.config.get("innovata", _INNOVATA)

    def run(self) -> pd.DataFrame:
        setup_logging()
        ic = self.innovata
        cols_cfg = ic.get("columns", [])
        headers = [c["name"] for c in cols_cfg]
        stn_src = _SOURCES.get("innovata_stn", {})
        tz_src = _SOURCES.get("innovata_tz", {})
        stn_path = APP.output_dir.parent / stn_src.get("file_path", "input/SWA_INV_STN.dat")
        tz_path = APP.output_dir.parent / tz_src.get("file_path", "input/SWA_INV_TZ.dat")
        for p, name in ((stn_path, "innovata_stn"), (tz_path, "innovata_tz")):
            if not p.is_file():
                raise FileNotFoundError(
                    f"Innovata source missing: {name} ({p}). "
                    f"Add input files per config sources.{name}.file_path"
                )
        stn_enc = stn_src.get("encoding", "latin-1")
        tz_enc = tz_src.get("encoding", "latin-1")
        stn_delim = _normalize_delim(stn_src.get("delimiter", "\t"))
        tz_delim = _normalize_delim(tz_src.get("delimiter", "\t"))

        stn_key = _station_key_field()
        st_fields, tz_fields = _stn_tz_fields()
        mode = ic.get("tz_period_mode", "all")
        as_of_str = ic.get("as_of_date")
        as_of = datetime.combine(
            date.fromisoformat(as_of_str) if as_of_str else date.today(), time(12, 0)
        )
        bad_char = re.compile(ic.get("special_char_pattern", r"[^\x20-\x7E]"))
        text_fields = ic.get("station_text_check_fields", ["STANAME", "CITYNAME"])
        batch_id = datetime.now().strftime("%Y%j%H%M%S")

        log.info("Loading Innovata TZ %s", tz_path)
        tz_index = load_tz_index(tz_path, tz_enc, tz_delim, tz_fields)
        log.info("TZ index keys=%d", len(tz_index))

        import csv

        def value(c: dict, stn: dict, tzr: dict):
            s = c.get("source")
            if s == "batch_id":
                return batch_id
            if s == "station":
                return (stn.get(c.get("field", ""), "") or "").strip()
            if s == "tz":
                return (tzr.get(c.get("field", ""), "") or "").strip()
            if s == "tz_offset_min":
                return gmt_minutes(tzr.get(c.get("field", ""), ""))
            raise ValueError(f"Unknown innovata column source '{s}'")

        errors = ErrorCapture(self.config, stage="innovata")
        ignores = ErrorCapture(self.config, stage="innovata")
        _ign_tpl = self.config.get("ignores", {}).get("templates", {})
        if _ign_tpl:
            ignores.templates = _ign_tpl
        rows: list[dict] = []
        stations: dict = {}
        counts = {"stations_read": 0, "stations_loaded": 0, "rows_written": 0,
                  "failed_special_chars": 0, "failed_no_timezone": 0,
                  "failed_no_current_period": 0}

        log.info("Loading Innovata STN %s (mode=%s as_of=%s)", stn_path, mode, as_of.date())
        with stn_path.open("r", encoding=stn_enc, newline="") as fh:
            for _, stn in enumerate(csv.DictReader(fh, delimiter=stn_delim), 2):
                counts["stations_read"] += 1
                code = (stn.get(stn_key, "") or "").strip()
                periods = tz_index.get(tuple((stn.get(f, "") or "").strip() for f in st_fields))
                stations[code] = {
                    "lat": innovata_lat(stn.get("LATITUDE", "")),
                    "lon": innovata_lon(stn.get("LONGITUDE", "")),
                    "country": (stn.get("CNTRYCODE", "") or "").strip(),
                    "offsets": {gmt_minutes(p.get("GMTVAR", "")) for p in periods} if periods else set(),
                }
                raw = "\t".join(str(v) for v in stn.values())
                hits = [f for f in text_fields if bad_char.search(stn.get(f, "") or "")]
                if hits:
                    counts["failed_special_chars"] += 1
                    errors.add("innovata_special_chars", code, raw, tag="innovata")
                    continue
                if not periods:
                    counts["failed_no_timezone"] += 1
                    errors.add("innovata_no_tz", code, raw, tag="innovata")
                    ignores.add("innovata_no_tz", code, raw, tag="innovata")
                    continue
                if mode == "current":
                    cur = [p for p in periods if p.get("_eff") and p["_eff"] <= as_of <= p.get("_disc")]
                    if not cur:
                        counts["failed_no_current_period"] += 1
                        errors.add("innovata_no_current", code, raw, tag="innovata")
                        continue
                    periods = [cur[-1]]
                for tzr in periods:
                    rows.append({c["name"]: value(c, stn, tzr) for c in cols_cfg})
                counts["stations_loaded"] += 1

        counts["rows_written"] = len(rows)
        df = pd.DataFrame(rows, columns=headers)
        if not df.empty:
            df = df.astype(str)
        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTPUT_CSV, index=False)
        log.info("Wrote %d rows (%d stations loaded / %d read) -> %s",
                 len(df), counts["stations_loaded"], counts["stations_read"], OUTPUT_CSV)

        # Central error report only — no separate innovata_errors.csv
        errors.save(APP.error_report, append=True)
        log.info(errors.summary("innovata"))
        ignores.save(APP.ignore_report, append=True)
        log.info("IGNORES stage=innovata count=%d", len(ignores))

        try:
            df.attrs["counts"] = counts
            df.attrs["stations"] = stations
            df.attrs["batch_id"] = batch_id
        except (AttributeError, ValueError, TypeError) as e:
            log.warning("Could not attach innovata attrs: %s", e)
        return df


def run() -> pd.DataFrame:
    return InnovataParser().run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    df = run()
    print(f"Innovata done: rows={len(df)} -> {OUTPUT_CSV}")
