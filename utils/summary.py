"""Stage summary strings shared by metadata rows and TAG logs."""
_JEP = {
    "airport": lambda s: f"jep_airports={s.get('airport_rows', s.get('total_airport_cnt', ''))} "
                         f"success={s.get('success_cnt', '')} "
                         f"failures={s.get('failure_cnt', '')} invalid={s.get('invalid_cnt', '')}",
    "gate": lambda s: f"jep_gate={s.get('gate_rows', '')} "
                      f"success={s.get('gate_success', s.get('gate_rows', ''))} "
                      f"failures={s.get('gate_failures', 0)} invalid=0",
    "runway": lambda s: f"jep_runway={s.get('runway_rows', '')} "
                        f"success={s.get('runway_success', s.get('runway_rows', ''))} "
                        f"failures={s.get('runway_failures', 0)} invalid=0",
}


class StageSummary:
    @staticmethod
    def skipped(reason):
        return f"skipped reason={reason}"

    @staticmethod
    def failed(reason):
        return f"failed reason={reason}"

    @staticmethod
    def jeppesen(tag, stats):
        return _JEP.get(tag, lambda s: "")(stats or {})

    @staticmethod
    def bts_airport(total, bts_n):
        return f"bts_total={total} matched={bts_n} src_loc=bts rows={bts_n}"

    @staticmethod
    def bts_nonlatlon(counts, jepp_n, fail_n):
        return f"missing_without={counts.get('without_filter', '')} " \
               f"missing_with={counts.get('with_filter', '')} " \
               f"src_loc=jepp rows={jepp_n} fail={fail_n} (inside BTS STG, SRC_LOC=jepp)"

    @staticmethod
    def innovata(counts):
        return f"innovata_stations={counts.get('stations_read', '')} " \
               f"loaded={counts.get('stations_loaded', '')} " \
               f"rows={counts.get('rows_written', '')} " \
               f"errors={counts.get('failed_special_chars', 0) + counts.get('failed_no_timezone', 0) + counts.get('failed_no_current_period', 0)} " \
               f"(special={counts.get('failed_special_chars', 0)} no_tz={counts.get('failed_no_timezone', 0)} no_cur={counts.get('failed_no_current_period', 0)})"

    @staticmethod
    def olson(counts):
        return f"olson_rows={counts.get('rows_read', '')} loaded={counts.get('loaded', '')} " \
               f"failed={counts.get('failed', 0)} dups={counts.get('duplicates', 0)} " \
               f"in_jep={counts.get('in_jeppesen', 0)} not_in_jep={counts.get('not_in_jeppesen', 0)} " \
               f"jep_without_olson={counts.get('jeppesen_without_olson', 0)}"

    @staticmethod
    def olson_pip(counts):
        return f"olson_pip_read={counts.get('rows_read', '')} loaded={counts.get('loaded', '')} " \
               f"failed={counts.get('failed', 0)} no_zone={counts.get('no_zone', 0)} " \
               f"bad_coords={counts.get('bad_coords', 0)}"

    @staticmethod
    def ignore(counts):
        return f"ignore_total={counts.get('total', '')} " + \
               " ".join(f"{k}={v}" for k, v in sorted(counts.items()) if k != "total")
