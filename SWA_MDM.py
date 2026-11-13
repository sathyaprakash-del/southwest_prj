"""SWA MDM pipeline (class-based, utils shared). Skips run when input DAT is unchanged."""
import logging
from pathlib import Path

from pipeline.stage import stage_bts
from utils.common import APP, Constants, FileHelper, setup_logging as _setup_logging
from utils.exceptions import ThresholdExceededError
from utils.metadata import write_metadata
from utils.summary import StageSummary

log = logging.getLogger("SWA_MDM")

_CONFIG = APP.config
INPUT_DAT = APP.input_dat
HISTORY_DIR = APP.history_dir
HISTORY_PREFIX = APP.history_prefix
LOG_FILE = APP.mdm_log

# OFF switch - keep file but do not run (do not delete pipeline/stage/stage_olson_pip.py)
ENABLE_OLSON_PIP = False


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


class MDMPipeline:
    """Class wrapper for full pipeline (uses utils + stage classes)."""
    def __init__(self):
        from pipeline.stage.stage_bts import BTSEnricher
        from pipeline.stage.stage_innovata import InnovataParser
        from pipeline.stage.stage_jeppesen import JeppesenParser
        from pipeline.stage.stage_olson import OlsonLoader
        # OFF - imported but not run unless ENABLE_OLSON_PIP
        from pipeline.stage.stage_olson_pip import OlsonPipLoader
        self.jep = JeppesenParser()
        self.enricher = BTSEnricher()
        self.innovata = InnovataParser()
        self.olson = OlsonLoader()
        self.olson_pip = OlsonPipLoader()

    def run(self) -> dict:
        from pipeline.stage.stage_bts import OUTPUT_CSV as BTS_OUT
        from pipeline.stage.stage_innovata import OUTPUT_CSV as INNOVATA_OUT
        from pipeline.stage.stage_olson import OUTPUT_CSV as OLSON_OUT
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
                write_metadata(result, {}, None)
                log.info("=== SWA_MDM pipeline SKIPPED (unchanged input) ===")
                return result
            log.info("Input DAT changed vs %s. Running pipeline.", latest.name)
        log.info("=== STEP 1/4: stage_jeppesen ===")
        try:
            stats = self.jep.run()
        except ThresholdExceededError as e:
            log.error("Pipeline halted: %s", e)
            result = {"skipped": False, "failed": True, "reason": str(e),
                      "stats": {}, "rows": 0, "history": "", "counts": {}}
            write_metadata(result, {}, None)
            log.info("=== SWA_MDM pipeline FAILED ===")
            return result
        log.info("Step 1 done: airports=%d success=%d failures=%d invalid=%d",
                 stats["total_airport_cnt"], stats["success_cnt"],
                 stats["failure_cnt"], stats["invalid_cnt"])
        for _tag in (Constants.TAG_AIRPORT, Constants.TAG_GATE, Constants.TAG_RUNWAY):
            log.info("TAG stage=%s tag=%s summary=%s", Constants.STAGE_JEPPESEN, _tag,
                     StageSummary.jeppesen(_tag, stats))
        log.info("=== STEP 2/4: stage_bts ===")
        merged = self.enricher.run()
        log.info("Step 2 done: joined rows=%d -> %s", len(merged), BTS_OUT)
        try:
            if Constants.COL_SRC_LOC in merged.columns:
                _loc = merged[Constants.COL_SRC_LOC].value_counts().to_dict()
            else:
                _loc = {}
            _counts = dict(getattr(merged, "attrs", {}).get("counts", {}))
            log.info("TAG stage=%s tag=%s summary=%s", Constants.STAGE_BTS, Constants.TAG_AIRPORT,
                     StageSummary.bts_airport(len(merged), int(_loc.get(Constants.SRC_BTS, 0))))
            log.info("TAG stage=%s tag=%s summary=%s", Constants.STAGE_BTS, Constants.TAG_NON_LATLON,
                     StageSummary.bts_nonlatlon(_counts, int(_loc.get(Constants.SRC_JEPP, 0)),
                                                int(_loc.get(Constants.SRC_FAIL, 0))))
        except (KeyError, AttributeError, ValueError, TypeError) as e:
            log.warning("TAG bts summary log failed: %s", e)
        log.info("=== STEP 3/4: stage_innovata ===")
        innovata_df = None
        innovata_counts: dict = {}
        try:
            innovata_df = self.innovata.run()
            innovata_counts = dict(getattr(innovata_df, "attrs", {}).get("counts", {}))
            log.info("Step 3 done: stations=%d loaded=%d rows=%d -> %s",
                     innovata_counts.get("stations_read", 0),
                     innovata_counts.get("stations_loaded", 0),
                     len(innovata_df), INNOVATA_OUT)
            log.info("TAG stage=%s tag=%s summary=%s", Constants.STAGE_INNOVATA, Constants.TAG_INNOVATA,
                     StageSummary.innovata(innovata_counts))
        except FileNotFoundError as e:
            log.warning("Innovata skipped (optional inputs missing): %s", e)
        log.info("=== STEP 4/4: stage_olson ===")
        olson_df = None
        olson_counts: dict = {}
        try:
            olson_df = self.olson.run()
            olson_counts = dict(getattr(olson_df, "attrs", {}).get("counts", {}))
            log.info("Step 4 done: rows=%d loaded=%d -> %s",
                     olson_counts.get("rows_read", 0), len(olson_df), OLSON_OUT)
            log.info("TAG stage=%s tag=%s summary=%s", Constants.STAGE_OLSON, Constants.TAG_OLSON,
                     StageSummary.olson(olson_counts))
        except FileNotFoundError as e:
            log.warning("Olson skipped (missing input): %s", e)
        # OFF - stage_olson_pip kept but not run
        olson_pip_df = None
        olson_pip_counts: dict = {}
        if ENABLE_OLSON_PIP:
            from pipeline.stage.stage_olson_pip import OUTPUT_CSV as OLSON_PIP_OUT
            try:
                olson_pip_df = self.olson_pip.run()
                olson_pip_counts = dict(getattr(olson_pip_df, "attrs", {}).get("counts", {}))
                log.info("Step 5 done: rows=%d loaded=%d -> %s",
                         olson_pip_counts.get("rows_read", 0), len(olson_pip_df), OLSON_PIP_OUT)
                log.info("TAG stage=%s tag=%s summary=%s", Constants.STAGE_OLSON_PIP, Constants.TAG_OLSON_PIP,
                         StageSummary.olson_pip(olson_pip_counts))
            except (FileNotFoundError, ValueError) as e:
                log.warning("Olson PIP skipped: %s", e)
        else:
            log.info("Stage olson_pip OFF (file kept, not run)")
        archived = archive_dat()
        counts: dict = {}
        try:
            counts = dict(getattr(merged, "attrs", {}).get("counts", {}))
        except (AttributeError, ValueError, TypeError) as e:
            log.warning("Pipeline counts unavailable, defaulting to empty: %s", e)
            counts = {}
        result = {"skipped": False, "stats": stats, "rows": len(merged), "history": str(archived), "counts": counts,
                  "innovata_counts": innovata_counts,
                  "innovata_rows": len(innovata_df) if innovata_df is not None else 0,
                  "olson_counts": olson_counts,
                  "olson_rows": len(olson_df) if olson_df is not None else 0,
                  "olson_pip_counts": olson_pip_counts,
                  "olson_pip_rows": len(olson_pip_df) if olson_pip_df is not None else 0}
        write_metadata(result, stats, merged, innovata_df, olson_df, olson_pip_df)
        log.info("=== SWA_MDM pipeline complete ===")
        return result


def run_pipeline() -> dict:
    return MDMPipeline().run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    setup_logging()
    try:
        stage_bts.setup_logging()
    except AttributeError:
        pass
    result = run_pipeline()
    if result.get("skipped"):
        print(f"Result: SKIPPED - both DAT files are same. See history: {result['history']}")
    else:
        print(f"Result: COMPLETED - pipeline ran. Archived: {result['history']}")
    print(result)
