#!/usr/bin/env python3
"""
Jeppesen parser, extended with the BTS, Innovata and Olson (Google time zone) steps.
Everything specific to the files and tables lives in test.json.

Stages run in the order used by the current load (KT document, section 2.0):

  1. Jeppesen  fixed-width file -> airport / gate / runway records
                                   (LOC_ARPT_STG, LOC_GATE_STG, LOC_RNWY_STG)
  2. BTS       csv              -> keep only current rows (AIRPORT_IS_LATEST = 1 and no
                                   AIRPORT_THRU_DATE), match on IATA code and fill the ten
                                   BTS_ARPT_* columns of LOC_ARPT_STG (section 2.3)
  3. Innovata  tab-delimited    -> stations joined to time zone rows -> LOC_TMZN_STG
  4. Olson     IANA time zone database (python libraries tzdata + timezonefinder) -> LOC_OLSON_ID_STG,
               one row per Jeppesen airport, keyed on ICAO code. The Google export csv is no longer
               needed (set olson.source to csv to read it again, or keep it only to compare).

How the files relate (config -> "joins"):
  Jeppesen IATA_ARPT_CDE = BTS AIRPORT = Innovata STACODE        (3-letter IATA code)
  Innovata station (CNTRYCODE, TZCODE) = Innovata timezone (CNTRYCODE, TZCODE)
  Jeppesen ICAO_ARPT_CDE = Olson ICAO_CDE                        (4-letter ICAO code)

Output file and column names are the Oracle staging tables. If the schema file
(MDM_DB_Tables_columns.csv) is present, every output is checked against it before
anything is written, and any value longer than the column size is rejected.

Outputs (output_directory): LOC_ARPT_STG, LOC_GATE_STG, LOC_RNWY_STG, LOC_TMZN_STG,
LOC_OLSON_ID_STG, LOC_BATCH_STAT (all .csv), AIRPORT_XREF.csv (one row per Jeppesen
airport showing which files matched), the error/reject reports and pipeline_status.json.

Needs: pip install tzdata timezonefinder   (see requirements.txt; tzdata 2026.6 = IANA release 2026f)

Usage:
    python jeppesen_parser.py
    python jeppesen_parser.py --config test.json --data-dir /path/to/inputs --output-dir out
    python jeppesen_parser.py --input other_file.dat --batch-id 2025319659
"""

import argparse
import csv
import importlib.resources as ir
import json
import math
import re
import sys
from collections import Counter, OrderedDict
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path


# ==========================================================================
# Config, schema and small helpers
# ==========================================================================
def load_config(path):
    with open(path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    for rt_name, rt in cfg["jeppesen"]["record_types"].items():
        for key in ("discriminators", "fields", "columns", "output_file", "table"):
            if key not in rt:
                raise ValueError(f"Config error: jeppesen record type '{rt_name}' has no '{key}'")
        for col in rt["columns"]:
            if col["source"] == "field" and col["field"] not in rt["fields"]:
                raise ValueError(f"Config error: {rt_name}.{col['name']} uses unknown field {col['field']}")
            if col["source"] == "concat":
                for f in col["fields"]:
                    if f not in rt["fields"]:
                        raise ValueError(f"Config error: {rt_name}.{col['name']} uses unknown field {f}")
    return cfg


def load_schema(path, encoding="utf-8-sig"):
    """{TABLE: OrderedDict(COLUMN -> max length or None)} in COLUMN_ID order."""
    rows = []
    with open(path, "r", encoding=encoding, newline="") as fh:
        for r in csv.DictReader(fh):
            rows.append(r)
    rows.sort(key=lambda r: (r["TABLE_NAME"], int(r["COLUMN_ID"])))
    schema = {}
    for r in rows:
        m = re.search(r"\((\d+)", r["DATA_TYPE"])
        schema.setdefault(r["TABLE_NAME"], OrderedDict())[r["COLUMN_NAME"].strip()] = int(m.group(1)) if m else None
    return schema


def planned_tables(cfg):
    """Column list of every output table, as the config will write it."""
    jt = cfg["jeppesen"]["record_types"]
    arpt = jt["airport"]
    t = {
        arpt["table"]: [c["name"] for c in arpt["columns"]] + [c["column"] for c in cfg["bts"]["staging_columns"]]
        + arpt.get("trailing_blank_columns", []),
        jt["gate"]["table"]: [c["name"] for c in jt["gate"]["columns"]],
        jt["runway"]["table"]: [c["name"] for c in jt["runway"]["columns"]],
        cfg["innovata"]["table"]: [c["name"] for c in cfg["innovata"]["columns"]],
        cfg["olson"]["table"]: list(cfg["olson"]["columns"]),
        cfg["batch_stat"]["table"]: list(cfg["batch_stat"]["columns"]),
    }
    return t


def check_schema(cfg, schema):
    """Return a list of problems between the planned output columns and the DB table columns."""
    problems = []
    for table, cols in planned_tables(cfg).items():
        if table not in schema:
            problems.append(f"{table}: table not found in the schema file")
            continue
        db = list(schema[table].keys())
        if cols != db:
            missing = [c for c in db if c not in cols]
            extra = [c for c in cols if c not in db]
            msg = f"{table}: output has {len(cols)} columns, table has {len(db)}"
            if missing:
                msg += f"; missing {missing}"
            if extra:
                msg += f"; not in table {extra}"
            if not missing and not extra:
                msg += "; same columns but different order"
            problems.append(msg)
    return problems


def length_error(schema, table, columns, values):
    """First value longer than its column size (bytes), or None."""
    limits = (schema or {}).get(table)
    if not limits:
        return None
    for c, v in zip(columns, values):
        n = limits.get(c)
        if n is not None and len(str(v).encode("utf-8")) > n:
            return f"{table}.{c} value is {len(str(v).encode('utf-8'))} bytes, column allows {n}"
    return None


class StageWriter:
    def __init__(self, path, columns, delimiter=","):
        self.fh = open(path, "w", newline="", encoding="utf-8")
        self.w = csv.writer(self.fh, delimiter=delimiter, lineterminator="\n")
        self.w.writerow(columns)
        self.rows = 0

    def write(self, values):
        self.w.writerow(values)
        self.rows += 1

    def close(self):
        self.fh.close()


def open_report(path, header):
    fh = open(path, "w", newline="", encoding="utf-8")
    w = csv.writer(fh, lineterminator="\n")
    w.writerow(header)
    return fh, w


def haversine_km(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(h))


def _dec(deg, minute, sec, hemi):
    x = deg + minute / 60.0 + sec / 3600.0
    return -x if hemi in ("S", "W") else x


def jeppesen_lat(v):          # N35195785  = N 35 19 57.85
    try:
        return _dec(int(v[1:3]), int(v[3:5]), int(v[5:9]) / 100.0, v[0]) if v and v[0] in "NS" else None
    except ValueError:
        return None


def jeppesen_lon(v):          # E004122115 = E 004 12 21.15
    try:
        return _dec(int(v[1:4]), int(v[4:6]), int(v[6:10]) / 100.0, v[0]) if v and v[0] in "EW" else None
    except ValueError:
        return None


def innovata_lat(v):          # 172109S = 17 21 09 S
    try:
        return _dec(int(v[0:2]), int(v[2:4]), int(v[4:6]), v[6]) if len(v) == 7 and v[6] in "NS" else None
    except ValueError:
        return None


def innovata_lon(v):          # 1453034W = 145 30 34 W
    try:
        return _dec(int(v[0:3]), int(v[3:5]), int(v[5:7]), v[7]) if len(v) == 8 and v[7] in "EW" else None
    except ValueError:
        return None


def to_float(v):
    try:
        return float(v) if v.strip() != "" else None
    except ValueError:
        return None


def gmt_minutes(v):
    """Innovata GMTVAR is hours*100 + minutes, signed: '930' = +570, '-300' = -180."""
    try:
        n = int(v)
    except ValueError:
        return ""
    sign = -1 if n < 0 else 1
    n = abs(n)
    return sign * ((n // 100) * 60 + n % 100)


def batch_stat_row(cfg, ctx, label, file_path, total, success, failure, status):
    return [ctx["batch_id"], cfg["run"].get("domain", "AIRPORT"), Path(file_path).name,
            str(total), str(success), str(failure), status, cfg["batch_stat"]["comments"].get(label, ""),
            "", cfg["run"].get("run_indicator", "")]


# ==========================================================================
# Stage 1: Jeppesen
# ==========================================================================
def jep_classify(line, record_types):
    for name, rt in record_types.items():
        if all(line[d["start"]:d["end"]] == d["equals"] for d in rt["discriminators"]):
            return name
    return None


def jep_cut(line, rt, trim, sql_escape):
    values = {}
    for fname, sp in rt["fields"].items():
        v = line[sp["start"]:sp["end"]]
        if trim:
            v = v.strip()
        if sql_escape and sp.get("escape_apostrophes", False):
            v = v.replace("'", "''")
        values[fname] = v
    return values


def jep_validate(values, rt):
    for rule in rt.get("validations", []):
        v = values[rule["field"]]
        if len(v) < rule.get("min_length", 0) or len(v) > rule.get("max_length", 10 ** 9):
            return f"{rule['field']} length {len(v)} not in [{rule.get('min_length', 0)}, {rule.get('max_length', 'inf')}]"
    return None


def jep_row(values, rt, batch_id):
    row = OrderedDict()
    for col in rt["columns"]:
        s = col["source"]
        if s == "batch_id":
            row[col["name"]] = batch_id
        elif s == "field":
            row[col["name"]] = values[col["field"]]
        elif s == "concat":
            row[col["name"]] = "".join(values[f] for f in col["fields"])
        elif s == "literal":
            row[col["name"]] = col.get("value", "")
        else:
            raise ValueError(f"Unknown column source '{s}' for {col['name']}")
    return row


def stage_jeppesen(cfg, ctx):
    jc = cfg["jeppesen"]
    src = cfg["sources"]["jeppesen"]
    rts = jc["record_types"]
    trim = jc.get("trim_values", True)
    esc = jc.get("sql_escape_apostrophes", False)
    out, schema = ctx["out_dir"], ctx["schema"]

    counts = {n: {"written": 0, "failed": 0} for n in rts}
    ignored = total = 0
    ctx["airports"] = []
    writers, efh = {}, None
    try:
        for name, rt in rts.items():
            if name != "airport":              # airports are written after the BTS update
                writers[name] = StageWriter(out / rt["output_file"], [c["name"] for c in rt["columns"]])
        efh, ew = open_report(out / jc["errors_file"], ["LINE_NO", "RECORD_TYPE", "ERROR", "DATA"])

        with open(ctx["paths"]["jeppesen"], "r", encoding=src.get("encoding", "latin-1"), newline="") as fh:
            for line_no, raw in enumerate(fh, 1):
                total += 1
                line = raw.rstrip("\r\n")
                rtype = jep_classify(line, rts)
                if rtype is None:
                    ignored += 1
                    continue
                rt = rts[rtype]
                values = jep_cut(line, rt, trim, esc)
                err = jep_validate(values, rt)
                row = None
                if not err:
                    row = jep_row(values, rt, ctx["batch_id"])
                    err = length_error(schema, rt["table"], list(row.keys()), list(row.values()))
                if err:
                    counts[rtype]["failed"] += 1
                    ew.writerow([line_no, rtype, err, line])
                    continue
                counts[rtype]["written"] += 1
                if rtype == "airport":
                    ctx["airports"].append({"values": values, "row": row})
                else:
                    writers[rtype].write(list(row.values()))
    finally:
        for w in writers.values():
            w.close()
        if efh:
            efh.close()

    success = sum(c["written"] for c in counts.values())
    failures = sum(c["failed"] for c in counts.values())
    airport_total = counts["airport"]["written"] + counts["airport"]["failed"]
    pct = jc.get("failure_threshold_percent")
    threshold = round(airport_total * pct / 100) if pct is not None else None
    ok = success > 0 and not (threshold is not None and failures > 0 and failures >= threshold)
    ctx["calculated_threshold"] = threshold
    ctx["stats"].append(batch_stat_row(cfg, ctx, "jeppesen", ctx["paths"]["jeppesen"], success + failures, success,
                                       failures, "COMPLETE" if ok else "ERROR"))
    print("Stage 1 Jeppesen")
    print(f"  lines read {total}, ignored {ignored}, failure threshold {threshold}")
    for n, c in counts.items():
        print(f"  {n:<8} written={c['written']:>6} failed={c['failed']}")
    return {"lines_read": total, "ignored": ignored, "counts": counts, "failure_threshold": threshold, "success": ok}


# ==========================================================================
# Stage 2: BTS
# ==========================================================================
def bts_rule_fails(rule, row):
    v = row.get(rule["field"], "").strip()
    if "equals" in rule and v != rule["equals"]:
        return f"{rule['field']} is '{v}', expected '{rule['equals']}'"
    if rule.get("is_empty") and v != "":
        return f"{rule['field']} is '{v}', expected empty"
    if rule.get("not_empty") and v == "":
        return f"{rule['field']} is empty"
    return None


def stage_bts(cfg, ctx):
    bc = cfg["bts"]
    src = cfg["sources"]["bts"]
    out, schema = ctx["out_dir"], ctx["schema"]
    key_field = cfg["joins"]["airport_code"]["bts_field"]
    thr = bc.get("location_check_km", 25)
    bts_cols = [c["column"] for c in bc["staging_columns"]]
    arpt_cfg = cfg["jeppesen"]["record_types"]["airport"]
    arpt_cols = [c["name"] for c in arpt_cfg["columns"]]

    # Jeppesen airports by IATA code (the BTS match key)
    by_iata = {}
    for ap in ctx["airports"]:
        iata = ap["values"].get("ATAIATADesignator", "")
        ap.update(iata=iata, jlat=jeppesen_lat(ap["row"]["JEP_ARPT_LAT"]), jlon=jeppesen_lon(ap["row"]["JEP_ARPT_LONG"]),
                  bts_values=None, bts_status="NO_IATA" if not iata else "NO_BTS_MATCH", blat=None, blon=None, km_jb=None)
        if iata:
            by_iata.setdefault(iata, []).append(ap)

    total = rejected = accepted = updated = duplicates = failed = unmatched = ambiguous = 0
    rej_fh = rej_w = None
    rej_cols = bc.get("reject_report_columns", [key_field])
    err_fh, err_w = open_report(out / bc["errors_file"], ["LINE_NO", "AIRPORT", "ERROR"] + rej_cols)
    try:
        if bc.get("report_rejects", True):
            rej_fh, rej_w = open_report(out / bc["rejects_file"], ["LINE_NO", "REASON"] + rej_cols)
        with open(ctx["paths"]["bts"], "r", encoding=src.get("encoding", "utf-8-sig"), newline="") as fh:
            for line_no, row in enumerate(csv.DictReader(fh, delimiter=src.get("delimiter", ",")), 2):
                total += 1
                reasons = [r for r in (bts_rule_fails(rule, row) for rule in bc["accept_rules"]) if r]
                if reasons:                       # not the latest / thru-dated: skipped
                    rejected += 1
                    if rej_w:
                        rej_w.writerow([line_no, "; ".join(reasons)] + [row.get(c, "") for c in rej_cols])
                    continue
                accepted += 1
                key = row[key_field].strip()
                aps = by_iata.get(key)
                if not aps:                       # no Jeppesen airport this batch: silent drop
                    unmatched += 1
                    continue
                if len(aps) != 1:                 # more than one row would be updated: silent drop
                    ambiguous += 1
                    continue
                ap = aps[0]
                if ap["bts_values"] is not None:  # already has BTS lat/long: duplicate, reported
                    duplicates += 1
                    err_w.writerow([line_no, key, "duplicate: airport already has BTS latitude/longitude"]
                                   + [row.get(c, "") for c in rej_cols])
                    continue
                vals = [(row[c["field"]].strip() if c.get("trim") else row[c["field"]]) for c in bc["staging_columns"]]
                err = length_error(schema, bc["table"], bts_cols, vals)
                if err:
                    failed += 1
                    err_w.writerow([line_no, key, err] + [row.get(c, "") for c in rej_cols])
                    continue
                blat, blon = to_float(row["LATITUDE"]), to_float(row["LONGITUDE"])
                km = haversine_km(ap["jlat"], ap["jlon"], blat, blon)
                mismatch = km is not None and km > thr
                if mismatch and bc.get("skip_on_location_mismatch", False):
                    ap["bts_status"] = "BTS_LOCATION_MISMATCH_SKIPPED"
                    ap.update(blat=blat, blon=blon, km_jb=km)
                    continue
                ap.update(bts_values=vals, blat=blat, blon=blon, km_jb=km,
                          bts_status="BTS_UPDATED_LOCATION_MISMATCH" if mismatch else "BTS_UPDATED")
                updated += 1
    finally:
        if rej_fh:
            rej_fh.close()
        err_fh.close()

    # --- write LOC_ARPT_STG: Jeppesen columns + ten BTS columns + trailing blank columns ---
    trailing = arpt_cfg.get("trailing_blank_columns", [])
    w = StageWriter(out / (arpt_cfg["output_file"]), arpt_cols + bts_cols + trailing)
    try:
        for ap in ctx["airports"]:
            w.write(list(ap["row"].values()) + (ap["bts_values"] or [""] * len(bts_cols)) + [""] * len(trailing))
    finally:
        w.close()

    status_counts = Counter(ap["bts_status"] for ap in ctx["airports"])
    ok = total > 0 and accepted > 0
    ctx["stats"].append(batch_stat_row(cfg, ctx, "bts", ctx["paths"]["bts"], updated + failed, updated, failed,
                                       "COMPLETE" if updated > 0 else "ERROR"))
    print("Stage 2 BTS")
    print(f"  rows read {total}: skipped (not current) {rejected}, accepted {accepted}")
    print(f"  accepted rows: updated an airport {updated}, no Jeppesen airport {unmatched}, "
          f"duplicate {duplicates}, ambiguous {ambiguous}, failed {failed}")
    for k, v in sorted(status_counts.items()):
        print(f"  airport {k:<30} {v}")
    return {"rows_read": total, "skipped_not_current": rejected, "accepted": accepted, "updated": updated,
            "no_jeppesen_airport": unmatched, "duplicates": duplicates, "ambiguous": ambiguous, "failed": failed,
            "airport_status": dict(status_counts), "success": ok}


# ==========================================================================
# Stage 3: Innovata
# ==========================================================================
def parse_dt(d, t):
    return datetime.strptime(d + t, "%d%m%Y%H%M")


def stage_innovata(cfg, ctx):
    ic = cfg["innovata"]
    out, schema = ctx["out_dir"], ctx["schema"]
    stn_src, tz_src = cfg["sources"]["innovata_stn"], cfg["sources"]["innovata_tz"]
    j = cfg["joins"]
    stn_key = j["airport_code"]["innovata_station_field"]
    st_fields, tz_fields = j["station_to_timezone"]["station_fields"], j["station_to_timezone"]["timezone_fields"]
    mode = ic.get("tz_period_mode", "all")
    bad_char = re.compile(ic["special_char_pattern"])
    as_of = datetime.combine(ctx["as_of"], time(12, 0))
    cols = [c["name"] for c in ic["columns"]]

    tz_index = {}
    with open(ctx["paths"]["innovata_tz"], "r", encoding=tz_src.get("encoding", "latin-1"), newline="") as fh:
        for row in csv.DictReader(fh, delimiter=tz_src.get("delimiter", "\t")):
            try:
                row["_eff"], row["_disc"] = parse_dt(row["EFFDATE"], row["EFFTIME"]), parse_dt(row["DISCDATE"], row["DISCTIME"])
            except ValueError:
                row["_eff"] = row["_disc"] = None
            tz_index.setdefault(tuple(row[f] for f in tz_fields), []).append(row)
    for rows in tz_index.values():
        rows.sort(key=lambda r: r["_eff"] or datetime.min)

    def value(c, stn, tzr):
        s = c["source"]
        if s == "batch_id":
            return ctx["batch_id"]
        if s == "station":
            return stn[c["field"]].strip()
        if s == "tz":
            return tzr[c["field"]].strip()
        if s == "tz_offset_min":
            return gmt_minutes(tzr[c["field"]])
        raise ValueError(f"Unknown innovata column source '{s}'")

    stations, counts = {}, Counter()
    w = StageWriter(out / ic["output_file"], cols)
    efh, ew = open_report(out / ic["errors_file"], ["LINE_NO", "STACODE", "ERROR", "DATA"])
    try:
        with open(ctx["paths"]["innovata_stn"], "r", encoding=stn_src.get("encoding", "latin-1"), newline="") as fh:
            for line_no, stn in enumerate(csv.DictReader(fh, delimiter=stn_src.get("delimiter", "\t")), 2):
                counts["stations_read"] += 1
                code = stn[stn_key].strip()
                periods = tz_index.get(tuple(stn[f] for f in st_fields))
                stations[code] = {"lat": innovata_lat(stn["LATITUDE"]), "lon": innovata_lon(stn["LONGITUDE"]),
                                  "country": stn["CNTRYCODE"].strip(),
                                  "offsets": {gmt_minutes(p["GMTVAR"]) for p in periods} if periods else set()}
                raw = "\t".join(stn.values())
                hits = [f for f in ic["station_text_check_fields"] if bad_char.search(stn[f])]
                if hits:
                    counts["failed_special_chars"] += 1
                    ew.writerow([line_no, code, "special characters in " + ",".join(hits), raw])
                    continue
                if not periods:
                    counts["failed_no_timezone"] += 1
                    ew.writerow([line_no, code, f"no timezone row for ({', '.join(stn[f] for f in st_fields)})", raw])
                    continue
                if mode == "current":
                    cur = [p for p in periods if p["_eff"] and p["_eff"] <= as_of <= p["_disc"]]
                    if not cur:
                        counts["failed_no_current_period"] += 1
                        ew.writerow([line_no, code, f"no timezone period covers {as_of.date()}", raw])
                        continue
                    periods = [cur[-1]]
                rows = [[value(c, stn, tzr) for c in ic["columns"]] for tzr in periods]
                bad = next((e for e in (length_error(schema, ic["table"], cols, r) for r in rows) if e), None)
                if bad:
                    counts["failed_too_long"] += 1
                    ew.writerow([line_no, code, bad, raw])
                    continue
                for r in rows:
                    w.write(r)
                counts["stations_loaded"] += 1
    finally:
        w.close()
        efh.close()

    ctx["stations"] = stations
    failed = counts["stations_read"] - counts["stations_loaded"]
    ok = counts["stations_loaded"] > 0
    ctx["stats"].append(batch_stat_row(cfg, ctx, "innovata", ctx["paths"]["innovata_stn"], counts["stations_read"],
                                       counts["stations_loaded"], failed, "COMPLETE" if ok else "ERROR"))
    print("Stage 3 Innovata")
    print(f"  stations read {counts['stations_read']}, loaded {counts['stations_loaded']}, rows written {w.rows} (mode={mode})")
    for k in ("failed_special_chars", "failed_no_timezone", "failed_no_current_period", "failed_too_long"):
        if counts[k]:
            print(f"  {k}: {counts[k]}")
    st = dict(counts)
    st.update(rows_written=w.rows, tz_period_mode=mode, success=ok)
    return st


# ==========================================================================
# Stage 4: Olson (Google time zone result)
# ==========================================================================
def stage_olson(cfg, ctx):
    oc = cfg["olson"]
    src = cfg["sources"]["olson"]
    out, schema = ctx["out_dir"], ctx["schema"]
    cols = oc["columns"]
    olson_re, raw_re = re.compile(oc["olson_id_pattern"]), re.compile(oc["rawoffset_pattern"])
    keep_batch = oc.get("batch_id_mode", "run") == "keep"

    seen, counts = {}, Counter()
    w = StageWriter(out / oc["output_file"], cols)
    efh, ew = open_report(out / oc["errors_file"], ["LINE_NO", "ICAO_CDE", "ERROR"])
    try:
        with open(ctx["paths"]["olson"], "r", encoding=src.get("encoding", "utf-8-sig"), newline="") as fh:
            for line_no, r in enumerate(csv.DictReader(fh, delimiter=src.get("delimiter", ",")), 2):
                counts["rows_read"] += 1
                icao, oid, raw = r["ICAO_CDE"].strip(), r["OLSON_ID"].strip(), r["RAWOFFSET"].strip()
                err = None
                if not (oc["icao_min_length"] <= len(icao) <= oc["icao_max_length"]):
                    err = f"ICAO_CDE length {len(icao)} not in [{oc['icao_min_length']}, {oc['icao_max_length']}]"
                elif not olson_re.match(oid):
                    err = f"OLSON_ID '{oid}' is blank or not in Area/City form"
                elif not raw_re.match(raw):
                    err = f"RAWOFFSET '{raw}' is not a whole number of seconds"
                elif icao in seen:
                    err = "duplicate ICAO_CDE (first row kept)"
                    counts["duplicates"] += 1
                if err:
                    counts["failed"] += 1
                    ew.writerow([line_no, icao, err])
                    continue
                batch = r["BATCHID"].strip() if keep_batch else ctx["batch_id"]
                if r["BATCHID"].strip() != ctx["batch_id"]:
                    counts["batch_id_differs_from_run"] += 1
                row = [batch, icao, oid, raw]
                err = length_error(schema, oc["table"], cols, row)
                if err:
                    counts["failed"] += 1
                    ew.writerow([line_no, icao, err])
                    continue
                seen[icao] = {"olson_id": oid, "raw": int(raw)}
                w.write(row)
                counts["loaded"] += 1
    finally:
        w.close()
        efh.close()

    jep_icao = {ap["row"]["ICAO_ARPT_CDE"] for ap in ctx["airports"]}
    counts["in_jeppesen"] = len(set(seen) & jep_icao)
    counts["not_in_jeppesen"] = len(set(seen) - jep_icao)
    counts["jeppesen_without_olson"] = len(jep_icao - set(seen))
    ctx["olson"] = seen
    ok = counts["loaded"] > 0
    ctx["stats"].append(batch_stat_row(cfg, ctx, "olson", ctx["paths"]["olson"], counts["rows_read"], counts["loaded"],
                                       counts["failed"], "COMPLETE" if ok else "ERROR"))
    print("Stage 4 Olson")
    print(f"  rows read {counts['rows_read']}, loaded {counts['loaded']}, failed {counts['failed']}")
    print(f"  Olson ICAO found in Jeppesen {counts['in_jeppesen']}, not in Jeppesen {counts['not_in_jeppesen']}, "
          f"Jeppesen airports without Olson {counts['jeppesen_without_olson']}")
    if counts["batch_id_differs_from_run"]:
        print(f"  BATCHID in the file differs from this run for {counts['batch_id_differs_from_run']} rows "
              f"(batch_id_mode = {oc.get('batch_id_mode', 'run')})")
    st = dict(counts)
    st["success"] = ok
    return st


# ==========================================================================
# Stage 4 (default): Olson ID from the IANA time zone database
# ==========================================================================
def load_zoneinfo(zone):
    """ZoneInfo read straight from the tzdata package, so the result always comes from that IANA
    release and not from whatever tz files the operating system happens to hold."""
    f = ir.files("tzdata.zoneinfo").joinpath(*zone.split("/"))
    with f.open("rb") as fh:
        return ZoneInfo.from_file(fh, key=zone)


def standard_offset_seconds(zi, year):
    """Standard (no daylight saving) offset from UTC in seconds, the same meaning as the Google
    rawOffset: the smallest offset the zone uses during the year (weekly samples)."""
    d0 = datetime(year, 1, 1, 12, tzinfo=timezone.utc)
    return min(int((d0 + timedelta(days=i)).astimezone(zi).utcoffset().total_seconds()) for i in range(0, 366, 7))


def iana_links():
    """{link name: target} from the tzdata package (lines 'L target link'), to tell a renamed
    zone (Asia/Calcutta -> Asia/Kolkata) from a genuinely different one."""
    links = {}
    for ln in ir.files("tzdata.zoneinfo").joinpath("tzdata.zi").read_text().splitlines():
        p = ln.split()
        if len(p) >= 3 and p[0] == "L":
            links[p[2]] = p[1]
    return links


def bts_point(ap, lat_i, lon_i):
    """The BTS coordinate pair for the Olson lookup: (point, error).
    Both values blank (or no BTS row)  -> (None, None): fall back to the Jeppesen pair.
    Present but not a valid number, out of range, or only one of the two present -> (None, error):
    the airport FAILS; it never falls back silently and the two sources are never mixed."""
    bv = ap["bts_values"]
    if bv is None:
        return None, None
    la_s, lo_s = bv[lat_i].strip(), bv[lon_i].strip()
    if la_s == "" and lo_s == "":
        return None, None
    if la_s == "" or lo_s == "":
        return None, f"BTS coordinate only partly present (latitude '{la_s}', longitude '{lo_s}')"
    la, lo = to_float(la_s), to_float(lo_s)
    if la is None or lo is None or not (-90 <= la <= 90) or not (-180 <= lo <= 180):
        return None, f"BTS coordinate present but invalid (latitude '{la_s}', longitude '{lo_s}')"
    return ("BTS", round(la, 8), round(lo, 8)), None


def olson_candidates(ap, station, ic, bts):
    """Coordinates to try, in order. Production rule: the BTS pair when it is not empty, else the
    Jeppesen pair (packed degrees/minutes/seconds already converted to decimal degrees, eight decimals).
    Guard (our addition): when the two are more than mismatch_km apart (a reused airport code), start
    with the one closer to the Innovata station. Innovata is only a last resort when no other
    coordinate lies on land."""
    jep = ("JEPPESEN", round(ap["jlat"], 8), round(ap["jlon"], 8)) if ap["jlat"] is not None and ap["jlon"] is not None else None
    stn = ("INNOVATA", station["lat"], station["lon"]) if station and station["lat"] is not None and station["lon"] is not None else None
    cands, note = [c for c in (bts, jep) if c], ""
    if bts and jep and ic.get("mismatch_guard", True) and ap["km_jb"] is not None and ap["km_jb"] > ic.get("mismatch_km", 25):
        if stn:
            db, dj = haversine_km(bts[1], bts[2], stn[1], stn[2]), haversine_km(jep[1], jep[2], stn[1], stn[2])
            cands = [bts, jep] if (db < dj and db <= 2 * ic.get("mismatch_km", 25)) else [jep, bts]
        else:
            cands = [jep, bts]
        note = f"BTS and Jeppesen are {ap['km_jb']:.0f} km apart; started with {cands[0][0]}"
    if stn and ic.get("use_innovata_last_resort", True):
        cands.append(stn)
    return cands, note


def stage_olson_iana(cfg, ctx):
    oc = cfg["olson"]
    ic = oc.get("iana", {})
    out, schema = ctx["out_dir"], ctx["schema"]
    cols = oc["columns"]
    try:
        import tzdata
        from timezonefinder import TimezoneFinder
        import timezonefinder
    except ImportError as e:
        print(f"ERROR: the IANA stage needs the python libraries tzdata and timezonefinder (pip install tzdata timezonefinder): {e}",
              file=sys.stderr)
        return {"success": False, "error": str(e)}
    tzv, tfv = tzdata.IANA_VERSION, timezonefinder.__version__
    warning = ""
    if tzv < ic.get("min_tzdata_version", "0"):
        warning = f"tzdata is IANA {tzv}, older than the required {ic['min_tzdata_version']}: upgrade with pip install -U tzdata"
    tf = TimezoneFinder()
    bts_names = [c["column"] for c in cfg["bts"]["staging_columns"]]
    lat_i, lon_i = bts_names.index("BTS_ARPT_LAT"), bts_names.index("BTS_ARPT_LONG")
    year = ic.get("offset_year") or ctx["as_of"].year
    zcache = {}

    def zone_offset(zone):
        if zone not in zcache:
            zcache[zone] = standard_offset_seconds(load_zoneinfo(zone), year)
        return zcache[zone]

    counts, srcs, seen = Counter(), Counter(), {}
    w = StageWriter(out / oc["output_file"], cols)
    efh, ew = open_report(out / oc["errors_file"], ["LINE_NO", "ICAO_CDE", "ERROR"])
    rfh, rw = open_report(out / ic.get("lookup_report_file", "olson_lookup_report.csv"),
                          ["ICAO_CDE", "IATA_ARPT_CDE", "LAT_USED", "LONG_USED", "COORD_SOURCE", "OLSON_ID", "RAWOFFSET_SEC", "NOTE"])
    try:
        for n, ap in enumerate(ctx["airports"], 1):
            counts["airports"] += 1
            icao, iata = ap["row"]["ICAO_ARPT_CDE"], ap["iata"]
            bts, bts_err = bts_point(ap, lat_i, lon_i)
            if bts_err:
                counts["failed"] += 1
                counts["bts_invalid"] += 1
                ew.writerow([n, icao, bts_err])
                continue
            cands, note = olson_candidates(ap, ctx["stations"].get(iata) if iata else None, ic, bts)
            zone = src = lat = lon = None
            for label, la, lo in cands:
                zone = tf.timezone_at_land(lat=la, lng=lo)       # None when the point is not on land
                if zone:
                    src, lat, lon = label, la, lo
                    if label != cands[0][0]:
                        note = (note + "; " if note else "") + f"{cands[0][0]} point not on land, used {label}"
                    break
            err = None
            if not cands:
                err = "no coordinates available"
            elif not zone:
                err = "no coordinate lies on land inside a time zone polygon"
            else:
                try:
                    raw = zone_offset(zone)
                except Exception as e:                           # zone name missing from the tzdata release
                    err = f"zone {zone} not readable from tzdata {tzv}: {type(e).__name__}"
            if not err:
                row = [ctx["batch_id"], icao, zone, str(raw)]
                err = length_error(schema, oc["table"], cols, row)
            if err:
                counts["failed"] += 1
                ew.writerow([n, icao, err])
                continue
            w.write(row)
            seen[icao] = {"olson_id": zone, "raw": raw}
            counts["loaded"] += 1
            srcs[src] += 1
            if note:
                counts["with_note"] += 1
            rw.writerow([icao, iata, f"{lat:.8f}", f"{lon:.8f}", src, zone, raw, note])
    finally:
        w.close(); efh.close(); rfh.close()

    # optional comparison with the earlier Google export
    cmp_stats = {}
    gpath = ctx["paths"].get("olson")
    if ic.get("compare_with_csv", False) and gpath:
        links = iana_links()
        canon = lambda z: links.get(z, z)
        g = {}
        with open(gpath, "r", encoding=cfg["sources"]["olson"].get("encoding", "utf-8-sig"), newline="") as fh:
            for r in csv.DictReader(fh, delimiter=cfg["sources"]["olson"].get("delimiter", ",")):
                g[r["ICAO_CDE"].strip()] = (r["OLSON_ID"].strip(), r["RAWOFFSET"].strip())
        cfh, cw = open_report(out / ic.get("compare_file", "olson_vs_google.csv"),
                              ["ICAO_CDE", "IANA_OLSON_ID", "GOOGLE_OLSON_ID", "IANA_RAWOFFSET", "GOOGLE_RAWOFFSET", "RESULT"])
        c = Counter()
        try:
            for icao, v in seen.items():
                if icao not in g:
                    c["not_in_google_file"] += 1
                    continue
                c["compared"] += 1
                gz, graw = g[icao]
                same_raw = str(v["raw"]) == graw
                same_zone = v["olson_id"] == gz
                same_link = canon(v["olson_id"]) == canon(gz)
                c["same_raw_offset"] += same_raw
                c["same_zone_name"] += same_zone
                c["same_zone_after_links"] += same_link
                if not same_raw or not same_link:
                    res = "DIFFERENT_RAW_OFFSET" if not same_raw else "SAME_OFFSET_OTHER_ZONE"
                    cw.writerow([icao, v["olson_id"], gz, v["raw"], graw, res])
                    c[res.lower()] += 1
        finally:
            cfh.close()
        cmp_stats = dict(c)

    ctx["olson"] = seen
    failed = counts["failed"]
    ok = counts["loaded"] > 0
    ctx["stats"].append(batch_stat_row(cfg, ctx, "olson", f"IANA_tzdata_{tzv}", counts["airports"], counts["loaded"], failed,
                                       "COMPLETE" if ok else "ERROR"))
    print("Stage 4 Olson (IANA)")
    print(f"  tzdata {tzv}, timezonefinder {tfv}, standard offset taken for {year}")
    print(f"  airports {counts['airports']}, loaded {counts['loaded']}, failed {failed}; coordinate source: "
          + ", ".join(f"{k}={v}" for k, v in sorted(srcs.items())))
    if counts["with_note"]:
        print(f"  {counts['with_note']} airports have a note in {ic.get('lookup_report_file', 'olson_lookup_report.csv')} (guard or fallback)")
    if warning:
        print("  WARNING: " + warning)
    if cmp_stats:
        print("  vs Google export: " + ", ".join(f"{k}={v}" for k, v in sorted(cmp_stats.items())))
    st = dict(counts)
    st.update(tzdata_version=tzv, timezonefinder_version=tfv, offset_year=year, coordinate_source=dict(srcs),
              vs_google_export=cmp_stats, warning=warning, success=ok)
    return st


# ==========================================================================
# Cross reference (one row per Jeppesen airport)
# ==========================================================================
def coord_check(km_jb, km_js, km_bs, thr):
    """Three-way comparison of Jeppesen (J), BTS (B) and Innovata (S) coordinates.
    A pair agrees when within thr km. Two or three agreeing pairs -> AGREE; one agreeing pair ->
    the source outside that pair is the outlier; none -> ALL_DISAGREE. One pair only -> AGREE/DISAGREE."""
    avail = {k: v <= thr for k, v in {"jb": km_jb, "js": km_js, "bs": km_bs}.items() if v is not None}
    if not avail:
        return "N/A"
    if len(avail) == 1:
        return "AGREE" if next(iter(avail.values())) else "DISAGREE"
    agreeing = [k for k, ok in avail.items() if ok]
    if len(agreeing) >= 2:
        return "AGREE"
    if not agreeing:
        return "ALL_DISAGREE"
    return {"jb": "INNOVATA_OUTLIER", "js": "BTS_OUTLIER", "bs": "JEPPESEN_OUTLIER"}[agreeing[0]]


def write_xref(cfg, ctx):
    thr = cfg["xref"].get("coordinate_check_km", 25)
    stations, olson = ctx["stations"], ctx.get("olson", {})
    header = ["ICAO_ARPT_CDE", "IATA_ARPT_CDE", "ARPT_NME", "IN_BTS", "IN_INNOVATA", "IN_OLSON", "BTS_MATCH_STATUS", "SOURCES",
              "KM_JEPPESEN_BTS", "KM_JEPPESEN_INNOVATA", "KM_BTS_INNOVATA", "COORD_CHECK",
              "BTS_COUNTRY", "INNOVATA_COUNTRY", "COUNTRY_CHECK", "OLSON_ID", "OLSON_RAWOFFSET_SEC", "OLSON_OFFSET_CHECK"]
    summary, osummary = Counter(), Counter()
    w = StageWriter(ctx["out_dir"] / cfg["xref"]["file"], header)
    try:
        for ap in ctx["airports"]:
            iata, icao = ap["iata"], ap["row"]["ICAO_ARPT_CDE"]
            stn = stations.get(iata) if iata else None
            has_bts = ap["blat"] is not None or ap["bts_values"] is not None
            slat, slon = (stn["lat"], stn["lon"]) if stn else (None, None)
            km_js = haversine_km(ap["jlat"], ap["jlon"], slat, slon)
            km_bs = haversine_km(ap["blat"], ap["blon"], slat, slon)
            cc = coord_check(ap["km_jb"], km_js, km_bs, thr)
            summary[cc] += 1
            ol = olson.get(icao)
            if not ol:
                oc_ = "N/A (no Olson)"
            elif not stn or not stn["offsets"]:
                oc_ = "N/A (no Innovata)"
            else:
                oc_ = "MATCH" if (ol["raw"] // 60) in stn["offsets"] else "DIFFERENT"
            osummary[oc_] += 1
            sources = "JEPPESEN" + ("+BTS" if has_bts else "") + ("+INNOVATA" if stn else "") + ("+OLSON" if ol else "")
            bc = ""
            if ap["bts_values"] is not None or ap["bts_status"].startswith("BTS_LOCATION"):
                bc = ctx["bts_country"].get(iata, "")
            ic = stn["country"] if stn else ""
            w.write([icao, iata, ap["row"]["ARPT_NME"], "Y" if has_bts else "N", "Y" if stn else "N", "Y" if ol else "N",
                     ap["bts_status"], sources,
                     "" if ap["km_jb"] is None else f"{ap['km_jb']:.2f}", "" if km_js is None else f"{km_js:.2f}",
                     "" if km_bs is None else f"{km_bs:.2f}", cc, bc, ic,
                     "N/A" if not (bc and ic) else ("AGREE" if bc == ic else "DIFFERENT"),
                     ol["olson_id"] if ol else "", ol["raw"] if ol else "", oc_])
    finally:
        w.close()
    return {"coord_check": dict(summary), "olson_offset_check": dict(osummary)}


# ==========================================================================
# Driver
# ==========================================================================
def main():
    ap = argparse.ArgumentParser(description="Split the Jeppesen file into the staging tables, then apply BTS, Innovata and Olson.")
    ap.add_argument("--config", default=str(Path(__file__).with_name("test.json")))
    ap.add_argument("--input", help="override the Jeppesen file path (sources.jeppesen.file_path)")
    ap.add_argument("--data-dir", help="override run.data_directory")
    ap.add_argument("--output-dir", help="override run.output_directory")
    ap.add_argument("--batch-id", help="override run.batch_id")
    ap.add_argument("--as-of", help="YYYY-MM-DD for innovata.tz_period_mode = current (default: today)")
    args = ap.parse_args()

    cfg = load_config(args.config)
    if args.input:
        cfg["sources"]["jeppesen"]["file_path"] = args.input
    run = cfg["run"]
    data_dir = Path(args.data_dir or run.get("data_directory", "."))
    out_dir = Path(args.output_dir or run["output_directory"])

    paths, missing = {}, []
    for name, s in cfg["sources"].items():
        p = Path(s["file_path"])
        p = p if p.is_absolute() else data_dir / p
        if p.is_file():
            paths[name] = p
        elif s.get("required", True):
            missing.append(f"{name}: {p}")
    if missing:   # all vendor files must be present before the pipeline starts
        sys.exit("Missing required source file(s), pipeline not started:\n  " + "\n  ".join(missing))

    schema = None
    if cfg.get("schema_check", {}).get("enabled", True) and "db_schema" in paths:
        schema = load_schema(paths["db_schema"], cfg["sources"]["db_schema"].get("encoding", "utf-8-sig"))
        problems = check_schema(cfg, schema)
        if problems:
            print("Schema check found differences between the output columns and the database tables:")
            for p in problems:
                print("  " + p)
            if cfg["schema_check"].get("fail_on_mismatch", True):
                sys.exit("Pipeline not started (set schema_check.fail_on_mismatch to false to run anyway).")
        else:
            print(f"Schema check passed for {len(planned_tables(cfg))} tables.")
    elif "db_schema" not in paths:
        print("Schema file not found: column and length checks skipped.")

    out_dir.mkdir(parents=True, exist_ok=True)
    as_of = date.fromisoformat(args.as_of or cfg["innovata"].get("as_of_date") or date.today().isoformat())
    batch_id = args.batch_id or run.get("batch_id") or datetime.now().strftime(run.get("batch_id_format", "%Y%j%H%M%S"))
    ctx = {"out_dir": out_dir, "paths": paths, "batch_id": batch_id, "as_of": as_of, "schema": schema, "stats": []}

    print(f"Batch id {batch_id} | output {out_dir.resolve()}")
    status = OrderedDict(batch_id=batch_id)
    exit_code = 0
    stages = [("jeppesen", stage_jeppesen, True), ("bts", stage_bts, True), ("innovata", stage_innovata, False)]
    olson_source = cfg["olson"].get("source", "iana")
    if olson_source == "iana":
        stages.append(("olson", stage_olson_iana, False))
    elif "olson" in paths:
        stages.append(("olson", stage_olson, False))
    else:
        print("Olson file not found and olson.source is csv: Olson stage skipped.")
    for name, fn, gates_next in stages:
        st = fn(cfg, ctx)
        status[name] = st
        if name == "bts":    # country of each accepted BTS airport, for the cross reference
            ctx["bts_country"] = {}
            sc = cfg["sources"]["bts"]
            with open(paths["bts"], "r", encoding=sc.get("encoding", "utf-8-sig"), newline="") as fh:
                for r in csv.DictReader(fh, delimiter=sc.get("delimiter", ",")):
                    if all(not bts_rule_fails(rule, r) for rule in cfg["bts"]["accept_rules"]):
                        ctx["bts_country"].setdefault(r[cfg["joins"]["airport_code"]["bts_field"]].strip(), r["AIRPORT_COUNTRY_CODE_ISO"])
        if not st["success"]:
            print(f"ERROR: stage '{name}' failed", file=sys.stderr)
            if gates_next:       # only Jeppesen and BTS decide whether the run goes on
                exit_code = 1
                break
            exit_code = max(exit_code, 2)

    # LOC_BATCH_STAT: one row per source file
    bs = cfg["batch_stat"]
    w = StageWriter(out_dir / bs["output_file"], bs["columns"])
    for row in ctx["stats"]:
        w.write(row)
    w.close()

    if exit_code != 1:
        if "stations" in ctx:
            ctx.setdefault("olson", {})
            status["xref"] = write_xref(cfg, ctx)
            print("Cross reference:")
            for grp, d in status["xref"].items():
                print(f"  {grp}: " + ", ".join(f"{k}={v}" for k, v in sorted(d.items())))
    with open(out_dir / run["status_file"], "w", encoding="utf-8") as fh:
        json.dump(status, fh, indent=2)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
