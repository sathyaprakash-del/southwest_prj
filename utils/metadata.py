"""Central OPS metadata writer (moved from SWA_MDM.py)."""
import csv
import logging
from datetime import datetime
from pathlib import Path

from utils.common import APP, Constants
from utils.summary import StageSummary

log = logging.getLogger("metadata")

METADATA_CSV = APP.metadata_csv
METADATA_COLUMNS = APP.metadata_columns


def write_metadata(result: dict, stats: dict | None = None, merged=None,
                   innovata_df=None, olson_df=None, olson_pip_df=None) -> Path:
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    err_file = str(APP.error_report)
    ign_file = str(APP.ignore_report)
    rows: list[dict] = []
    if result.get("skipped"):
        rows.append(
            {"run_timestamp": run_ts, "status": Constants.STATUS_SKIPPED,
             "stage": Constants.STAGE_PIPELINE, "tag": "",
             "summary": StageSummary.skipped(result.get("reason", "unchanged")),
             "output": "", "error_report": "", "ignore_report": ""})
    elif result.get("failed"):
        rows.append(
            {"run_timestamp": run_ts, "status": Constants.STATUS_FAILED,
             "stage": Constants.STAGE_PIPELINE, "tag": "",
             "summary": StageSummary.failed(result.get("reason", "threshold")),
             "output": "", "error_report": "", "ignore_report": ""})
    else:
        stats = stats or {}
        for tag in (Constants.TAG_AIRPORT, Constants.TAG_GATE, Constants.TAG_RUNWAY):
            rows.append(
                {"run_timestamp": run_ts, "status": Constants.STATUS_COMPLETED,
                 "stage": Constants.STAGE_JEPPESEN, "tag": tag,
                 "summary": StageSummary.jeppesen(tag, stats),
                 "output": str(APP.stage_files[tag]),
                 "error_report": err_file, "ignore_report": ign_file})
        total = len(merged) if merged is not None else 0
        try:
            if merged is not None and Constants.COL_SRC_LOC in merged.columns:
                loc = merged[Constants.COL_SRC_LOC].value_counts().to_dict()
            else:
                loc = {}
        except (KeyError, AttributeError, ValueError, TypeError) as e:
            log.warning("SRC_LOC counts unavailable, defaulting to zeros: %s", e)
            loc = {}
        counts = result.get("counts", {})
        bts_n = int(loc.get(Constants.SRC_BTS, 0))
        jepp_n = int(loc.get(Constants.SRC_JEPP, 0))
        fail_n = int(loc.get(Constants.SRC_FAIL, 0))
        rows.append(
            {"run_timestamp": run_ts, "status": Constants.STATUS_COMPLETED,
             "stage": Constants.STAGE_BTS, "tag": Constants.TAG_AIRPORT,
             "summary": StageSummary.bts_airport(total, bts_n),
             "output": str(APP.bts_output),
             "error_report": "", "ignore_report": ign_file})
        rows.append(
            {"run_timestamp": run_ts, "status": Constants.STATUS_COMPLETED,
             "stage": Constants.STAGE_BTS, "tag": Constants.TAG_NON_LATLON,
             "summary": StageSummary.bts_nonlatlon(counts, jepp_n, fail_n),
             "output": str(APP.bts_output),
             "error_report": str(APP.error_report), "ignore_report": ign_file})
        innov_counts = result.get("innovata_counts", {})
        try:
            if innovata_df is not None:
                innov_counts = dict(getattr(innovata_df, "attrs", {}).get("counts", {}) or innov_counts)
        except (AttributeError, ValueError, TypeError) as e:
            log.warning("Innovata counts unavailable: %s", e)
        if innovata_df is not None or innov_counts:
            rows.append(
                {"run_timestamp": run_ts, "status": Constants.STATUS_COMPLETED,
                 "stage": Constants.STAGE_INNOVATA, "tag": Constants.TAG_INNOVATA,
                 "summary": StageSummary.innovata(innov_counts),
                 "output": str(APP.innovata_output),
                 "error_report": str(APP.error_report), "ignore_report": ign_file})
        olson_counts = result.get("olson_counts", {})
        try:
            if olson_df is not None:
                olson_counts = dict(getattr(olson_df, "attrs", {}).get("counts", {}) or olson_counts)
        except (AttributeError, ValueError, TypeError) as e:
            log.warning("Olson counts unavailable: %s", e)
        if olson_df is not None or olson_counts:
            rows.append(
                {"run_timestamp": run_ts, "status": Constants.STATUS_COMPLETED,
                 "stage": Constants.STAGE_OLSON, "tag": Constants.TAG_OLSON,
                 "summary": StageSummary.olson(olson_counts),
                 "output": str(APP.olson_output),
                 "error_report": str(APP.error_report), "ignore_report": ign_file})
        olson_pip_counts = result.get("olson_pip_counts", {})
        try:
            if olson_pip_df is not None:
                olson_pip_counts = dict(getattr(olson_pip_df, "attrs", {}).get("counts", {}) or olson_pip_counts)
        except (AttributeError, ValueError, TypeError) as e:
            log.warning("Olson PIP counts unavailable: %s", e)
        if olson_pip_df is not None or olson_pip_counts:
            rows.append(
                {"run_timestamp": run_ts, "status": Constants.STATUS_COMPLETED,
                 "stage": Constants.STAGE_OLSON_PIP, "tag": Constants.TAG_OLSON_PIP,
                 "summary": StageSummary.olson_pip(olson_pip_counts),
                 "output": str(APP.olson_pip_output),
                 "error_report": str(APP.error_report), "ignore_report": ign_file})
    METADATA_CSV.parent.mkdir(parents=True, exist_ok=True)
    needs_header = True
    if METADATA_CSV.is_file():
        try:
            with METADATA_CSV.open("r", encoding="utf-8") as f:
                needs_header = (f.readline().strip() != ",".join(METADATA_COLUMNS))
            if needs_header:
                METADATA_CSV.unlink()
        except (OSError, UnicodeDecodeError) as e:
            log.warning("Metadata header check failed, will rewrite header: %s", e)
            needs_header = True
    is_new = not METADATA_CSV.is_file()
    with METADATA_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=METADATA_COLUMNS)
        if is_new or needs_header:
            w.writeheader()
        w.writerows(rows)
    for r in rows:
        log.info("Metadata appended -> %s: %s", METADATA_CSV, r)
    return METADATA_CSV
