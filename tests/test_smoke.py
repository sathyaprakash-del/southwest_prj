"""Smoke test: outputs, counts, TIMEZONE (run: python -m unittest discover -s tests)."""
import csv
import pathlib
import sys
import unittest

import pandas as pd

from utils.common import APP

OUT = APP.output_dir


def _latest(pattern: str) -> pathlib.Path:
    files = sorted((OUT / "stage").glob(pattern))
    assert files, f"missing output/stage/{pattern}"
    return max(files, key=lambda p: p.stat().st_mtime)


def _read(name):
    # timestamped outputs: LOC_X_STG_YYYYMMDD_HHMM.csv
    p = OUT / name
    if p.is_file():
        return pd.read_csv(p, dtype=str, keep_default_na=False)
    stem = pathlib.Path(name).stem  # e.g. LOC_AIRPORT_STG
    return pd.read_csv(_latest(f"{stem}_*.csv"), dtype=str, keep_default_na=False)


class TestOutputs(unittest.TestCase):
    def test_files_exist(self):
        # timestamped: check at least one match per pattern
        for pat in ["LOC_AIRPORT_STG_*.csv", "LOC_GATE_STG_*.csv", "LOC_RUNWAY_STG_*.csv",
                    "LOC_TMZN_STG_*.csv", "LOC_OLSON_ID_STG_*.csv"]:
            self.assertTrue(list((OUT / "stage").glob(pat)), f"missing output/stage/{pat}")
        for f in ["ERROR_REPORT.csv", "IGNORE_REPORT.csv", "OPS_Process_Data.csv"]:
            self.assertTrue((OUT / f).is_file(), f"missing output/{f}")

    def test_no_legacy_artifacts(self):
        bts = _read("stage/LOC_AIRPORT_STG.csv")
        self.assertNotIn("LAT_SRC", bts.columns)
        self.assertNotIn("COORDINATES", bts.columns)
        self.assertFalse((OUT / "LOC_AIRPORT_BTS_MISSING.csv").exists())
        self.assertFalse((OUT / "ERROR_BTS.csv").exists())
        # BTS now overwrites STG itself, separate file must not exist
        self.assertFalse((OUT / "stage/LOC_AIRPORT_BTS_STG.csv").exists())


class TestCounts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.air = _read("stage/LOC_AIRPORT_STG.csv")
        cls.bts = _read("stage/LOC_AIRPORT_STG.csv")

    def test_airport_rows(self):
        self.assertEqual(len(self.air), 5890)

    def test_jep_prefix_cols(self):
        for c in ["JEP_DEC_LAT", "JEP_DEC_LON", "JEP_COR_LAT", "JEP_COR_LON"]:
            self.assertIn(c, self.air.columns)

    def test_bts_rows(self):
        self.assertEqual(len(self.bts), 5890)

    def test_bts_extra_cols(self):
        for c in ["BATCHID", "FINAL_LAT", "FINAL_LON", "BTS_CONV", "TIMEZONE"]:
            self.assertIn(c, self.bts.columns)
        self.assertNotIn("SRC_LOC", self.bts.columns)

    def test_batchid_format(self):
        for v in self.bts["BATCHID"].unique():
            self.assertRegex(v, r"^\d{12}$", f"BATCHID {v} not YYYYMMDDHHMM")

    def test_timezone_populated(self):
        self.assertEqual(int((self.bts["TIMEZONE"] != "").sum()), 5890)


class TestMetadata(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(OUT / "OPS_Process_Data.csv") as f:
            cls.rows = list(csv.DictReader(f))

    def test_row_count(self):
        self.assertGreaterEqual(len(self.rows), 4)

    def test_stages_present(self):
        self.assertGreaterEqual({r["stage"] for r in self.rows}, {"jeppesen", "bts"})

    def test_non_latlon_tag(self):
        self.assertTrue(any(r["tag"] == "non-latlon" for r in self.rows if r["stage"] == "bts"))

    def test_jeppesen_tags(self):
        tags = {r["tag"] for r in self.rows if r["stage"] == "jeppesen"}
        self.assertGreaterEqual(tags, {"airport", "gate", "runway"})


class TestErrors(unittest.TestCase):
    def test_error_schema(self):
        from utils.common import APP
        expected = APP.config["errors"]["headers"] + ["run_timestamp", "stage", "tag"]
        with open(OUT / "ERROR_REPORT.csv") as fh:
            self.assertEqual(next(csv.reader(fh)), expected)

    def test_config_shape(self):
        from utils.common import APP
        cfg = APP.config
        self.assertEqual(set(cfg["jeppesen"]["record_types"]), {"airport", "gate", "runway"})
        for rt, rc in cfg["jeppesen"]["record_types"].items():
            for k in ("description", "output_file", "discriminators", "fields",
                      "validations", "columns"):
                self.assertIn(k, rc, f"{rt} missing {k}")
            for d in rc["discriminators"]:
                self.assertIn("equals", d)
        join = cfg["joins"]["airport_code"]
        self.assertEqual((join["jeppesen_column"], join["bts_field"]), ("IATA_ARPT_CDE", "AIRPORT"))
        self.assertTrue(cfg["bts"]["accept_rules"])

    def test_error_report_empty_on_current_data(self):
        # Jeppesen/BTS/Olson clean, only 16 Innovata ZZ no-timezone expected
        df = _read("ERROR_REPORT.csv")
        self.assertEqual((df["stage"] == "innovata").sum(), 16)
        self.assertEqual((df["stage"] != "innovata").sum(), 0)

    def test_check_lat_lon(self):
        from utils.error_capture import check_lat_lon
        lat, lon, problems = check_lat_lon("36.69", "-122.5")
        self.assertEqual((lat, lon, problems), (36.69, -122.5, []))
        lat, lon, problems = check_lat_lon("N35195785", "E004122115")
        self.assertEqual(problems, [])
        self.assertAlmostEqual(lat, 35.33273611, places=5)
        lat, lon, problems = check_lat_lon("", "")
        self.assertEqual((lat, lon), (None, None))
        _, _, problems = check_lat_lon("abc", "4.2")
        self.assertEqual(problems, ["lat"])
        _, _, problems = check_lat_lon("12.0", "200")
        self.assertEqual(problems, ["lon"])

    def test_capture_roundtrip(self):
        import tempfile
        from utils.error_capture import ErrorCapture
        ec = ErrorCapture(stage="bts")
        ec.add("icao_invalid", "KXYZ", "rawline", tag="airport")
        ec.add("bts_lat_invalid", "KXYZ", "abc|1.0", tag="non-latlon")
        df = ec.to_dataframe()
        self.assertEqual(list(df.columns), ["BUSINESSNAME", "BUSINESSKEY", "DATA", "EXCEPTION_TYPE",
                                            "ERROR_CDE", "run_timestamp", "stage", "tag"])
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[1]["ERROR_CDE"], "BTSLAT-ERR-1001")
        self.assertEqual(df.iloc[1]["stage"], "bts")
        self.assertEqual(ec.summary("bts"), "ERRORS stage=bts count=2")
        with tempfile.TemporaryDirectory() as tmp:
            p = ec.save(pathlib.Path(tmp) / "err.csv")
            self.assertTrue(p.is_file())
            self.assertEqual(len(pd.read_csv(p, dtype=str)), 2)


if __name__ == "__main__":
    sys.exit(0 if unittest.main(exit=False).result.wasSuccessful() else 1)
