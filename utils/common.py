"""Shared utilities: logging, config, coordinates, files (used by all stages)."""
import hashlib
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "config.json"


def load_config(path: Path = CONFIG_PATH) -> dict:
    """Load JSON config (single source; yml kept as reference copy)."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def setup_logging(name: str, log_file: Path) -> Path:
    """Console + file logging without duplicate handlers (centralized)."""
    log_file = Path(log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    logger.propagate = False
    # ensure root doesn't duplicate to console twice
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return log_file


class CoordConverter:
    """Packed DMS <-> decimal + DMS display + BTS decimal validation."""
    @staticmethod
    def packed_to_decimal(packed: str, is_lat: bool = True) -> float | None:
        if packed is None:
            return None
        s = str(packed).strip().upper()
        if len(s) < 7:
            return None
        hemi = s[0]
        if is_lat and hemi not in ("N", "S"):
            return None
        if not is_lat and hemi not in ("E", "W"):
            return None
        digits = s[1:]
        if not digits.isdigit():
            return None
        try:
            if is_lat:
                deg = int(digits[:-6]) if len(digits) > 6 else int(digits[:2])
            else:
                deg = int(digits[:-6]) if len(digits) > 6 else int(digits[:3])
            minute = int(digits[-6:-4])
            sec = int(digits[-4:]) / 100.0
            dec = deg + minute / 60.0 + sec / 3600.0
            if hemi in ("S", "W"):
                dec = -dec
            return round(dec, 8)
        except (ValueError, IndexError):
            return None

    @staticmethod
    def packed_to_dms(packed: str, is_lat: bool = True) -> str | None:
        if packed is None:
            return None
        s = str(packed).strip().upper()
        if len(s) < 7:
            return None
        hemi = s[0]
        if is_lat and hemi not in ("N", "S"):
            return None
        if not is_lat and hemi not in ("E", "W"):
            return None
        digits = s[1:]
        if not digits.isdigit():
            return None
        try:
            if is_lat:
                deg = int(digits[:-6]) if len(digits) > 6 else int(digits[:2])
                deg_str = f"{deg:02d}"
            else:
                deg = int(digits[:-6]) if len(digits) > 6 else int(digits[:3])
                deg_str = f"{deg:03d}"
            minute = int(digits[-6:-4])
            sec = int(digits[-4:]) / 100.0
            return f"{deg_str}° {minute:02d}' {sec:05.2f}\" {hemi}"
        except (ValueError, IndexError):
            return None

    @staticmethod
    def valid_bts_decimal(val, is_lat: bool, logger=None) -> float | None:
        import pandas as pd
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        s = str(val).strip()
        if s == "" or s.lower() == "nan":
            return None
        try:
            f = float(s)
        except ValueError:
            if logger:
                logger.warning("BTS present-but-invalid value %r; will fall back to Jeppesen", val)
            return None
        try:
            if pd.isna(f):
                return None
        except (TypeError, ValueError):
            pass
        if is_lat and not (-90.0 <= f <= 90.0):
            if logger:
                logger.warning("BTS latitude out of range %r; will fall back to Jeppesen", val)
            return None
        if not is_lat and not (-180.0 <= f <= 180.0):
            if logger:
                logger.warning("BTS longitude out of range %r; will fall back to Jeppesen", val)
            return None
        return f


class FileHelper:
    """History archive + misc file ops."""
    @staticmethod
    def md5_of(path: Path, chunk_size: int = 1024 * 1024) -> str:
        h = hashlib.md5()
        with Path(path).open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def archive_dat(src: Path, history_dir: Path, prefix: str, logger=None) -> Path:
        history_dir = Path(history_dir)
        history_dir.mkdir(parents=True, exist_ok=True)
        dest = history_dir / f"{prefix}{datetime.now().strftime('%Y%m%d_%H%M%S')}.dat"
        shutil.copy2(src, dest)
        if logger:
            logger.info("Archived %s -> %s", src, dest)
        return dest


class AppConfig:
    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path else CONFIG_PATH
        self.config = load_config(self.path)
        run = self.config["run"]
        dirs = self.config["directories"]
        logs = self.config["logs"]
        sources = self.config["sources"]
        for name, src in sources.items():
            if src.get("required") and not (BASE_DIR / src["file_path"]).is_file():
                raise FileNotFoundError(f"Required source missing: {name} ({src['file_path']})")
        self.skip_lines = run["skip_lines"]
        self.threshold_percent = self.config["jeppesen"]["failure_threshold_percent"]
        self.output_dir = BASE_DIR / dirs["output"]
        self.stage_dir = BASE_DIR / dirs["stage"]
        self.history_dir = BASE_DIR / dirs["history"]
        self.history_prefix = dirs["history_prefix"]
        self.logs_dir = BASE_DIR / dirs["logs"]
        self.jeppesen_log = BASE_DIR / logs["jeppesen"]
        self.bts_log = BASE_DIR / logs["bts"]
        self.mdm_log = BASE_DIR / logs["mdm"]
        self.input_dat = BASE_DIR / sources["jeppesen"]["file_path"]
        self.input_encoding = sources["jeppesen"].get("encoding", "utf-8")
        self.bts_csv = BASE_DIR / sources["bts"]["file_path"]
        self.bts_encoding = sources["bts"].get("encoding", "utf-8")
        self.bts_delimiter = sources["bts"].get("delimiter", ",")
        self.stage_files = {rt: self.stage_dir / rc["output_file"]
                            for rt, rc in self.config["jeppesen"]["record_types"].items()}
        self.airport_stg = self.stage_files["airport"]
        self.bts_output = self.stage_dir / self.config["bts"]["output_file"]
        self.error_report = self.output_dir / self.config["errors"]["report_file"]
        # Innovata (optional files, required=false, independent no join)
        innov = self.config.get("innovata", {})
        innov_out = innov.get("output_file", "stage/LOC_TMZN_STG.csv")
        self.innovata_output = (self.output_dir / innov_out) if str(innov_out).startswith("stage/") else (self.stage_dir / Path(innov_out).name)
        self.innovata_stn = BASE_DIR / sources.get("innovata_stn", {}).get("file_path", "input/SWA_INV_STN.dat")
        self.innovata_tz = BASE_DIR / sources.get("innovata_tz", {}).get("file_path", "input/SWA_INV_TZ.dat")
        self.innovata_log = BASE_DIR / logs.get("innovata", "logs/stage_innovata.log")
        # Olson (csv direct source)
        olson_cfg = self.config.get("olson", {})
        olson_src = sources.get("olson", {})
        self.olson_csv = BASE_DIR / olson_src.get("file_path", "input/loc_olson_id_stg_export.csv")
        self.olson_encoding = olson_src.get("encoding", "utf-8-sig")
        self.olson_delimiter = olson_src.get("delimiter", ",")
        olson_out = olson_cfg.get("output_file", "LOC_OLSON_ID_STG.csv")
        self.olson_output = (self.output_dir / olson_out) if str(olson_out).startswith("stage/") else (self.stage_dir / Path(olson_out).name)
        self.olson_log = BASE_DIR / logs.get("olson", "logs/stage_olson.log")
        # Olson PIP (pip-only from enriched STG, no input csv)
        olson_pip_cfg = self.config.get("olson_pip", {})
        olson_pip_out = olson_pip_cfg.get("output_file", "LOC_OLSON_ID_PIP_STG.csv")
        self.olson_pip_output = (self.output_dir / olson_pip_out) if str(olson_pip_out).startswith("stage/") else (self.stage_dir / Path(olson_pip_out).name)
        self.olson_pip_log = BASE_DIR / logs.get("olson_pip", "logs/stage_olson_pip.log")
        # Ignore report (same 9 cols as error report)
        ignores_cfg = self.config.get("ignores", {})
        self.ignore_report = self.output_dir / ignores_cfg.get("report_file", "IGNORE_REPORT.csv")
        self.ignore_headers = ignores_cfg.get("headers", self.config.get("errors", {}).get("headers", []))
        meta = self.config["outputs"]["metadata"]
        self.metadata_csv = BASE_DIR / meta["file"]
        self.metadata_columns = meta["columns"]


APP = AppConfig()


class Constants:
    COL_PRODUCT_ID = "PRODUCTID"
    COL_JEP_DEC_LAT = "JEP_DEC_LAT"
    COL_JEP_DEC_LON = "JEP_DEC_LON"
    COL_JEP_COR_LAT = "JEP_COR_LAT"
    COL_JEP_COR_LON = "JEP_COR_LON"
    COL_JEP_ARPT_LAT = "JEP_ARPT_LAT"
    COL_JEP_ARPT_LONG = "JEP_ARPT_LONG"
    COL_BTS_LAT = "bts_LATITUDE"
    COL_BTS_LON = "bts_LONGITUDE"
    COL_FINAL_LAT = "FINAL_LAT"
    COL_FINAL_LON = "FINAL_LON"
    COL_SRC_LOC = "SRC_LOC"
    COL_BTS_CONV = "BTS_CONV"
    COL_TIMEZONE = "TIMEZONE"
    SRC_BTS = "bts"
    SRC_JEPP = "jepp"
    SRC_FAIL = "fail"
    STAGE_JEPPESEN = "jeppesen"
    STAGE_BTS = "bts"
    STAGE_INNOVATA = "innovata"
    STAGE_OLSON = "olson"
    STAGE_OLSON_PIP = "olson_pip"
    STAGE_PIPELINE = "PIPELINE"
    TAG_AIRPORT = "airport"
    TAG_GATE = "gate"
    TAG_RUNWAY = "runway"
    TAG_NON_LATLON = "non-latlon"
    TAG_INNOVATA = "innovata"
    TAG_OLSON = "olson"
    TAG_OLSON_PIP = "olson_pip"
    STATUS_COMPLETED = "COMPLETED"
    STATUS_SKIPPED = "SKIPPED"
    STATUS_FAILED = "FAILED"
    ERROR_RUN_TIMESTAMP = "run_timestamp"
    ERROR_STAGE = "stage"
    ERROR_TAG = "tag"
