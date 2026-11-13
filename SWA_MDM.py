"""SWA MDM pipeline (config-only jobs, no hardcode, no folder scan)."""
import importlib
import logging
from pathlib import Path

from utils.common import APP, FileHelper, setup_logging as _setup_logging
from utils.exceptions import ThresholdExceededError
from utils.metadata import write_metadata

log = logging.getLogger("SWA_MDM")

INPUT_DAT = APP.input_dat
HISTORY_DIR = APP.history_dir
HISTORY_PREFIX = APP.history_prefix
LOG_FILE = APP.mdm_log


def setup_logging() -> Path:
    return _setup_logging("SWA_MDM", LOG_FILE)


def archive_dat() -> Path:
    return FileHelper.archive_dat(INPUT_DAT, HISTORY_DIR, HISTORY_PREFIX, log)


def find_latest_history() -> Path | None:
    if not HISTORY_DIR.is_dir():
        return None
    candidates = sorted(HISTORY_DIR.glob(f"{HISTORY_PREFIX}*.dat"), reverse=True)
    return candidates[0] if candidates else None


def is_same_as_last() -> tuple:
    latest = find_latest_history()
    if latest is None:
        return False, None, None, None
    if not INPUT_DAT.is_file():
        raise FileNotFoundError(f"Input DAT not found: {INPUT_DAT}")
    input_hash = FileHelper.md5_of(INPUT_DAT)
    history_hash = FileHelper.md5_of(latest)
    return input_hash == history_hash, latest, input_hash, history_hash


def get_jobs() -> list[dict]:
    """Only config pipeline.jobs drives the run. No folder scan, no disabled list."""
    cfg = APP.config.get("pipeline", {})
    return list(cfg.get("jobs", []))


def _run_job_module(module: str, batch_id: str | None = None):
    mod = importlib.import_module(f"pipeline.stage.{module}")
    if hasattr(mod, "run") and callable(mod.run):
        try:
            return mod.run(batch_id=batch_id) if batch_id else mod.run()
        except TypeError:
            return mod.run()
    for attr in dir(mod):
        if attr.startswith("_"):
            continue
        obj = getattr(mod, attr)
        try:
            try:
                inst = obj(batch_id=batch_id) if batch_id else obj()
            except TypeError:
                inst = obj()
        except TypeError:
            continue
        if hasattr(inst, "run") and callable(inst.run):
            try:
                return inst.run(batch_id=batch_id)
            except TypeError:
                return inst.run()
    raise RuntimeError(f"No runnable run() in {module}")


class MDMPipeline:
    """No stage classes loaded here. Jobs come only from config."""

    def run(self) -> dict:
        if not INPUT_DAT.is_file():
            raise FileNotFoundError(f"Input DAT not found: {INPUT_DAT}")
        is_same, latest, input_hash, history_hash = is_same_as_last()
        if latest is None:
            log.info("No history found. Running pipeline (first run).")
        else:
            log.info("Last history: %s", latest)
            log.info("Input MD5:   %s (%s)", input_hash, INPUT_DAT)
            log.info("History MD5: %s (%s)", history_hash, latest)
            if is_same:
                msg = (f"SKIPPED: Input DAT is identical to latest history "
                       f"({latest.name}, md5={history_hash}). Pipeline NOT run.")
                log.warning(msg)
                print(msg)
                result = {"skipped": True, "reason": "unchanged", "history": str(latest)}
                write_metadata(result, {})
                log.info("=== SWA_MDM pipeline SKIPPED (unchanged input) ===")
                return result
            log.info("Input DAT changed vs %s. Running pipeline.", latest.name)

        jobs = get_jobs()
        total = len(jobs)
        log.info("Jobs from config (%d): %s", total, [j.get("module") for j in jobs])
        from utils.common import run_batch_id
        batch_id = run_batch_id()
        log.info("Run batch_id (YYYYMMDD_HHMM, no sec): %s", batch_id)

        results: dict = {}

        for i, job in enumerate(jobs, 1):
            module = job.get("module", "")
            name = job.get("name", module)
            gates_next = bool(job.get("gates_next", False))
            log.info("=== STEP %d/%d: %s (%s) ===", i, total, name, module)
            try:
                out = _run_job_module(module, batch_id)
            except ThresholdExceededError as e:
                log.error("Pipeline halted: %s", e)
                result = {"skipped": False, "failed": True, "reason": str(e),
                          "stats": {}, "rows": 0, "history": ""}
                write_metadata(result, {})
                log.info("=== SWA_MDM pipeline FAILED ===")
                return result
            except FileNotFoundError as e:
                if gates_next:
                    log.error("Required job %s missing input, halting: %s", module, e)
                    result = {"skipped": False, "failed": True, "reason": str(e),
                              "stats": {}, "rows": 0, "history": ""}
                    write_metadata(result, {})
                    return result
                log.warning("%s skipped (missing input): %s", module, e)
                continue

            results[module] = out
            try:
                _mod = importlib.import_module(f"pipeline.stage.{module}")
                _out_csv = getattr(_mod, "OUTPUT_CSV", "")
            except ImportError:
                _out_csv = ""
            if isinstance(out, dict):
                log.info("Step done: %s -> %s stats=%s", module, _out_csv, out.get("total_airport_cnt", out))
            else:
                log.info("Step done: %s -> %s rows=%s", module, _out_csv,
                         len(out) if hasattr(out, "__len__") else type(out))

        archived = archive_dat()
        result = {"skipped": False, "history": str(archived)}
        write_metadata(result, results)
        log.info("=== SWA_MDM pipeline complete ===")
        return result


def run_pipeline() -> dict:
    return MDMPipeline().run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    setup_logging()
    result = run_pipeline()
    if result.get("skipped"):
        print(f"Result: SKIPPED - both DAT files are same. See history: {result['history']}")
    else:
        print(f"Result: COMPLETED - pipeline ran. Archived: {result['history']}")
    print(result)
