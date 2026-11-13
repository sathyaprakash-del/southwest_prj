"""Central OPS metadata writer - generic, no per-stage hardcode."""
import csv
import importlib
import logging
from datetime import datetime
from pathlib import Path

from utils.common import APP, Constants

log = logging.getLogger("metadata")

METADATA_CSV = APP.metadata_csv
METADATA_COLUMNS = APP.metadata_columns


def write_metadata(result: dict, results: dict | None = None) -> Path:
    from utils.summary import StageSummary
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows: list[dict] = []
    if result.get("skipped"):
        rows.append(
            {"run_timestamp": run_ts, "status": Constants.STATUS_SKIPPED,
             "stage": "PIPELINE", "tag": "",
             "summary": StageSummary.skipped(result.get("reason", "unchanged")),
             "output": "", "error_report": "", "ignore_report": ""})
    elif result.get("failed"):
        rows.append(
            {"run_timestamp": run_ts, "status": Constants.STATUS_FAILED,
             "stage": "PIPELINE", "tag": "",
             "summary": StageSummary.failed(result.get("reason", "threshold")),
             "output": "", "error_report": "", "ignore_report": ""})
    else:
        for module, out in (results or {}).items():
            try:
                mod = importlib.import_module(f"pipeline.stage.{module}")
            except ImportError as e:
                log.warning("Metadata skip %s: %s", module, e)
                continue
            fn = getattr(mod, "metadata_rows", None)
            if not callable(fn):
                log.warning("Metadata skip %s: no metadata_rows()", module)
                continue
            try:
                rows.extend(fn(out, run_ts) or [])
            except (ValueError, TypeError, AttributeError) as e:
                log.warning("Metadata %s failed: %s", module, e)
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
