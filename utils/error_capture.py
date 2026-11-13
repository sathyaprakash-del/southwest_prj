"""Shared lat/lon check and error capture used by jeppesen and bts stages."""
from datetime import datetime

import pandas as pd

from utils.common import Constants, CoordConverter, load_config
from utils.exceptions import PipelineError


def _to_decimal(value, is_lat):
    s = str(value).strip()
    if s == "" or s.lower() == "nan":
        return None, "blank"
    if s[0].upper() in ("N", "S", "E", "W") and s[1:].isdigit():
        v = CoordConverter.packed_to_decimal(s, is_lat)
        return (v, "ok") if v is not None else (None, "invalid")
    try:
        return float(s), "ok"
    except ValueError:
        return None, "invalid"


def check_lat_lon(raw_lat, raw_lon):
    lat, lat_state = _to_decimal(raw_lat, True)
    lon, lon_state = _to_decimal(raw_lon, False)
    if lat_state == "blank" and lon_state == "blank":
        return None, None, []
    problems = []
    if lat is None or lat_state != "ok" or not -90.0 <= lat <= 90.0:
        problems.append("lat")
        lat = None
    if lon is None or lon_state != "ok" or not -180.0 <= lon <= 180.0:
        problems.append("lon")
        lon = None
    return lat, lon, problems


class ErrorCapture:
    def __init__(self, config=None, *, stage="", run_timestamp=None):
        cfg = config if config is not None else load_config()
        err_cfg = cfg.get("errors", {})
        self.templates = err_cfg.get("templates", {})
        # BATCH_TYPE removed for now, re-add in future via config headers/templates
        fallback = ["BUSINESSNAME", "BUSINESSKEY", "DATA", "EXCEPTION_TYPE", "ERROR_CDE"]
        self.headers = list(err_cfg.get("headers", fallback))
        self.headers += [Constants.ERROR_RUN_TIMESTAMP, Constants.ERROR_STAGE, Constants.ERROR_TAG]
        self.stage = stage
        self.run_timestamp = run_timestamp or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.rows = []

    def add(self, template, business_key, data, tag=""):
        t = self.templates.get(template, {})
        self.rows.append({
            "BUSINESSNAME": t.get("business_name", ""),
            "BUSINESSKEY": business_key,
            "DATA": data,
            "EXCEPTION_TYPE": t.get("exception_type", ""),
            "ERROR_CDE": t.get("error_cde", ""),
            Constants.ERROR_RUN_TIMESTAMP: self.run_timestamp,
            Constants.ERROR_STAGE: self.stage,
            Constants.ERROR_TAG: tag,
        })
        return self.rows[-1]

    def check(self, raw_lat, raw_lon, *, source, record_id, tag="", data=None):
        lat, lon, problems = check_lat_lon(raw_lat, raw_lon)
        for kind in problems:
            self.add(f"{source.lower()}_{kind}_invalid", record_id,
                     data if data is not None else f"{raw_lat}|{raw_lon}", tag)
        return lat, lon, problems

    def to_dataframe(self):
        df = pd.DataFrame(self.rows, columns=self.headers)
        return df.astype(str) if df.empty else df

    def save(self, path, append=False):
        from pathlib import Path
        path = Path(path)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if append and path.is_file():
                with path.open("r", encoding="utf-8") as f:
                    if f.readline().strip() != ",".join(self.headers):
                        raise PipelineError(f"Error report header mismatch, refusing to append: {path}")
                if not self.rows:
                    return path
                self.to_dataframe().to_csv(path, mode="a", header=False, index=False)
            else:
                self.to_dataframe().to_csv(path, index=False)
        except OSError as e:
            raise PipelineError(f"Cannot write error report {path}: {e}") from e
        return path

    def summary(self, stage=None):
        return f"ERRORS stage={stage or self.stage} count={len(self.rows)}"

    def __len__(self):
        return len(self.rows)

    def __bool__(self):
        return bool(self.rows)
