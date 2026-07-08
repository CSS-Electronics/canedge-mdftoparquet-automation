#!/usr/bin/env python3
"""Unit tests for the optional routing.json resolver (modules/routing.py).

Deterministic checks of the routing decisions - rule resolution, the recording-date cutoff, the
catch-all fallback, and malformed-config handling - without needing to decode real MDF files.

Run:  python local-testing/test_routing_unit.py
"""
import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from modules.routing import resolve_routing_rule, resolve_target_bucket  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("routing-unit")

DEFAULT = "myfleet-parquet"
CONFIG = {
    "2F6913DB": {"output_bucket": "myfleet-custa-parquet", "from_date": "2026-05-01"},
    "7512BE4D": {"output_bucket": "myfleet-custb-parquet"},               # no from_date => any date
    "BADRULE1": {"from_date": "2020-01-01"},                             # missing output_bucket
    "BADDATE1": {"output_bucket": "x-parquet", "from_date": "May 2026"},  # malformed from_date
}

_failures = []


def check(name, got, exp):
    if got == exp:
        print(f"  ok  : {name}")
    else:
        print(f"  FAIL: {name}: expected {exp!r}, got {got!r}")
        _failures.append(name)


print("resolve_routing_rule:")
check("absent config ([] from download_json_file)", resolve_routing_rule("2F6913DB", [], log), None)
check("empty config ({})", resolve_routing_rule("2F6913DB", {}, log), None)
check("unmapped device -> None", resolve_routing_rule("DEADBEEF", CONFIG, log), None)
check("missing output_bucket -> None", resolve_routing_rule("BADRULE1", CONFIG, log), None)
check("malformed from_date -> None", resolve_routing_rule("BADDATE1", CONFIG, log), None)
check("valid with from_date", resolve_routing_rule("2F6913DB", CONFIG, log),
      {"output_bucket": "myfleet-custa-parquet", "from_date": "2026-05-01"})
check("valid without from_date", resolve_routing_rule("7512BE4D", CONFIG, log),
      {"output_bucket": "myfleet-custb-parquet", "from_date": ""})

ruleA = resolve_routing_rule("2F6913DB", CONFIG, log)   # from_date 2026-05-01
ruleB = resolve_routing_rule("7512BE4D", CONFIG, log)   # no from_date

p_after = "2F6913DB/CAN2_GnssSpeed/2026/06/15/00000001_00000001.parquet"
p_before = "2F6913DB/CAN2_GnssSpeed/2026/01/10/00000001_00000001.parquet"
p_event = "aggregations/events/2026/06/15/2F6913DB_CAN2_GnssSpeed_Speed_overspeed_00000001.parquet"

print("resolve_target_bucket:")
check("no rule -> default", resolve_target_bucket(p_after, None, DEFAULT, log), DEFAULT)
check("on/after cutoff -> customer bucket", resolve_target_bucket(p_after, ruleA, DEFAULT, log), "myfleet-custa-parquet")
check("before cutoff -> default catch-all", resolve_target_bucket(p_before, ruleA, DEFAULT, log), DEFAULT)
check("event-table path routes by date", resolve_target_bucket(p_event, ruleA, DEFAULT, log), "myfleet-custa-parquet")
check("no from_date routes any date", resolve_target_bucket(p_before, ruleB, DEFAULT, log), "myfleet-custb-parquet")
check("unparseable path -> default", resolve_target_bucket("weird.parquet", ruleA, DEFAULT, log), DEFAULT)

if _failures:
    print(f"\n{len(_failures)} FAILED: {_failures}")
    sys.exit(1)
print("\nALL ROUTING UNIT TESTS PASSED")
