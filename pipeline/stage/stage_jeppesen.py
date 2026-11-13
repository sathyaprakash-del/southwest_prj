"""Stage Jeppesen DAT file into LOC_*_STG CSVs using pandas (class-based, utils shared)."""
import logging
import sys
from pathlib import Path

import pandas as pd

try:
    from utils.common import APP, CoordConverter, setup_logging as _setup_logging
    from utils.error_capture import ErrorCapture
    from utils.exceptions import ThresholdExceededError
except ImportError:  # direct script run: python pipeline/stage_jeppesen.py
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from utils.common import APP, CoordConverter, setup_logging as _setup_logging
    from utils.error_capture import ErrorCapture
    from utils.exceptions import ThresholdExceededError

log = logging.getLogger(__name__)

INPUT_DAT = APP.input_dat
LOG_FILE = APP.jeppesen_log
SKIP_LINES = APP.skip_lines
THRESHOLD_PERCENT = APP.threshold_percent


def setup_logging() -> Path:
    return _setup_logging(__name__, LOG_FILE)


_DERIVED_CONV = {
    "decimal": lambda v, lat: CoordConverter.packed_to_decimal(v, lat),
    "dms": lambda v, lat: CoordConverter.packed_to_dms(v, lat),
}


def extract_field(line: str, start: int, end: int, trim: bool = True) -> str:
    if len(line) < end:
        return ""
    val = line[start:end]
    return val.strip() if trim else val


escape_apostrophe = lambda value: value.replace("'", "''")

validate_icao = lambda value, min_length, max_length: min_length <= len(value) <= max_length


classify_record = lambda line, record_types: next(
    (rt for rt, rc in record_types.items()
     if rc.get("discriminators") and all(line[d.get("start", 0):d.get("end", 0)] == d.get("equals", "")
                                         for d in rc["discriminators"])),
    None)


def parse_record(line: str, record_type: str, config: dict) -> tuple[dict, str, dict]:
    jp = config["jeppesen"]
    rc = jp["record_types"][record_type]
    trim = jp.get("trim_values", True)
    sql_default = jp.get("sql_escape_apostrophes", False)
    row: dict[str, str] = {}
    raw: dict[str, str] = {}
    for name, f in rc["fields"].items():
        val = extract_field(line, f["start"], f["end"], trim)
        if f.get("escape_apostrophes", sql_default):
            val = escape_apostrophe(val)
        raw[name] = val
    for col in rc.get("columns", []):
        src = col.get("source", "field")
        if src == "field":
            row[col["name"]] = raw.get(col.get("field", ""), "")
        elif src == "concat":
            row[col["name"]] = "".join(raw.get(p, "") for p in col.get("fields", []))
        elif src == "literal":
            row[col["name"]] = col.get("value", "")
        elif src == "derived":
            v = _DERIVED_CONV[col["kind"]](row.get(col.get("source_field", ""), ""), col.get("is_lat", True))
            row[col["name"]] = "" if v is None else str(v)
    return row, raw.get("AirportICAOIdentifier", ""), raw


def _make_ignore_capture(config) -> ErrorCapture:
    cap = ErrorCapture(config, stage="jeppesen")
    ign = (config or {}).get("ignores", {})
    if ign.get("templates"):
        cap.templates = ign["templates"]
    return cap


def process_jeppesen_file(
    input_dat: Path = INPUT_DAT, config: dict | None = None, skip_lines: int = SKIP_LINES
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, dict[str, int], pd.DataFrame]:
    if config is None:
        config = APP.config
    jp = config["jeppesen"]
    record_types = jp["record_types"]
    enc = config.get("sources", {}).get("jeppesen", {}).get("encoding", "utf-8")
    buckets: dict[str, list[dict]] = {k: [] for k in record_types}
    errors = ErrorCapture(config, stage="jeppesen")
    ignores = _make_ignore_capture(config)
    stats = {"total_airport_cnt": 0, "success_cnt": 0, "failure_cnt": 0, "invalid_cnt": 0,
             "airport_success": 0, "gate_success": 0, "runway_success": 0,
             "airport_failures": 0, "gate_failures": 0, "runway_failures": 0}

    with input_dat.open("r", encoding=enc, errors="replace") as f:
        for line_num, raw_line in enumerate(f, 1):
            if line_num <= skip_lines:
                ignores.add("jep_skip", str(line_num), raw_line.rstrip("\r\n")[:123], tag="skip")
                continue
            line = raw_line.rstrip("\r\n")
            if len(line) < 22:
                stats["invalid_cnt"] += 1
                ignores.add("jep_short", str(line_num), line[:123], tag="invalid")
                continue
            record_type = classify_record(line, record_types)
            if record_type is None:
                stats["invalid_cnt"] += 1
                ignores.add("jep_unknown", str(line_num), line[:123], tag="invalid")
                continue
            rc = record_types[record_type]
            row, icao, raw = parse_record(line, record_type, config)
            ok = all(validate_icao(raw.get(v.get("field", ""), ""), v.get("min_length", 0),
                                   v.get("max_length", 999)) for v in rc.get("validations", []))
            if not ok:
                stats["failure_cnt"] += 1
                stats[f"{record_type}_failures"] = stats.get(f"{record_type}_failures", 0) + 1
                errors.add("icao_invalid", raw.get("AirportICAOIdentifier", icao), line, tag=record_type)
                continue
            errors.check(row.get("JEP_ARPT_LAT", ""), row.get("JEP_ARPT_LONG", ""), source="JEP",
                         record_id=raw.get("AirportICAOIdentifier", icao), tag=record_type, data=line)
            buckets[record_type].append(row)
            stats["success_cnt"] += 1
            stats[f"{record_type}_success"] = stats.get(f"{record_type}_success", 0) + 1
            if rc.get("counted"):
                stats["total_airport_cnt"] += 1

    frames: dict[str, pd.DataFrame] = {}
    for rtype, rc in record_types.items():
        headers = [c["name"] for c in rc.get("columns", [])]
        df = pd.DataFrame(buckets[rtype], columns=headers)
        df = df.reindex(columns=headers).fillna("").astype(str)
        frames[rtype] = df

    error_df = errors.to_dataframe()
    log.info(errors.summary("jeppesen"))
    ignore_df = ignores.to_dataframe()
    log.info("IGNORES stage=jeppesen count=%d", len(ignores))
    return frames, error_df, stats, ignore_df


def check_threshold(total_airports: int, failures: int, threshold_percent: float = THRESHOLD_PERCENT) -> bool:
    threshold = round((total_airports * threshold_percent) / 100) if total_airports > 0 else 0
    if failures >= threshold:
        raise ThresholdExceededError(
            f"CRITICAL [JEP-REC-10001]: FAILURE THRESHOLD EXCEEDED (failures={failures} >= threshold={threshold})")
    log.info("Threshold OK (failures=%d < threshold=%d)", failures, threshold)
    return True


def save_outputs(
    frames: dict[str, pd.DataFrame], error_df: pd.DataFrame, ignore_df: pd.DataFrame | None = None
) -> None:
    APP.stage_dir.mkdir(parents=True, exist_ok=True)
    for rtype, df in frames.items():
        out = APP.stage_dir / APP.config["jeppesen"]["record_types"][rtype]["output_file"]
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        log.info("Wrote %d rows -> %s", len(df), out)
    err_path = APP.error_report
    err_path.parent.mkdir(parents=True, exist_ok=True)
    error_df.to_csv(err_path, index=False)
    log.info("Wrote %d errors -> %s", len(error_df), err_path)
    if ignore_df is not None:
        ign_path = APP.ignore_report
        ign_path.parent.mkdir(parents=True, exist_ok=True)
        ignore_df.to_csv(ign_path, index=False)
        log.info("Wrote %d ignores -> %s", len(ignore_df), ign_path)


class JeppesenParser:
    def __init__(self, config: dict | None = None):
        self.config = config or APP.config

    def run(self) -> dict:
        setup_logging()
        log.info("Loading config %s", APP.path)
        log.info("Parsing %s", INPUT_DAT)
        frames, error_df, stats, ignore_df = process_jeppesen_file(INPUT_DAT, self.config, SKIP_LINES)
        save_outputs(frames, error_df, ignore_df)
        stats["airport_rows"] = len(frames.get("airport", []))
        stats["gate_rows"] = len(frames.get("gate", []))
        stats["runway_rows"] = len(frames.get("runway", []))
        log.info("Jeppesen done: airports=%d success=%d failures=%d invalid=%d (airport=%d gate=%d runway=%d)",
                 stats["total_airport_cnt"], stats["success_cnt"], stats["failure_cnt"], stats["invalid_cnt"],
                 stats["airport_rows"], stats["gate_rows"], stats["runway_rows"])
        check_threshold(stats["total_airport_cnt"], stats["failure_cnt"], THRESHOLD_PERCENT)
        return stats


def run() -> dict[str, int]:
    return JeppesenParser().run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    setup_logging()
    run()
