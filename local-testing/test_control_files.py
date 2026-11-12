#!/usr/bin/env python3
"""Unit tests for the control file validation (modules/control_files.py) and related v4.3.0 fixes.

- The example control files in json-examples/ and local-testing/local-input-bucket/ are valid
- Known mistakes are reported (error or warning) with a clear message
- Backlog entry normalization and batch grouping
- Missing objects are classified as 'not found' on every cloud (INFO, not ERROR)
- Only publish_notification writes the text 'NEW EVENT' (Google/Azure event alerts match on it)

Run:  python local-testing/test_control_files.py
"""
import copy
import glob
import json
import logging
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from modules.control_files import CONTROL_FILES, ControlFileError, check, classify_backlog_entry, parse, validate  # noqa: E402
from modules.cloud_functions import is_not_found_error  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("control-files-unit")

_failures = []


def check_eq(name, got, exp):
    if got == exp:
        print(f"  ok  : {name}")
    else:
        print(f"  FAIL: {name}: expected {exp!r}, got {got!r}")
        _failures.append(name)


def load(name):
    with open(os.path.join(REPO, "json-examples", name), encoding="utf-8") as f:
        return json.load(f)


def expect(name, file_name, data, level, text, cloud=None):
    """data must give an issue of this level whose text contains 'text'."""
    issues = validate(file_name, data, cloud=cloud)
    hits = [i for i in issues if i.level == level and text in str(i)]
    check_eq(name, bool(hits), True)
    if not hits:
        print(f"        issues: {[f'{i.level}: {i}' for i in issues]}")


print("Example files are valid:")
for folder in ("json-examples", os.path.join("local-testing", "local-input-bucket")):
    for path in sorted(glob.glob(os.path.join(REPO, folder, "*.json"))):
        name = os.path.basename(path)
        if name not in CONTROL_FILES:
            continue
        with open(path, "rb") as f:
            _, issues = parse(name, f.read(), cloud="Amazon")
        check_eq(f"{folder}/{name}", [str(i) for i in issues if i.level == "error"], [])

print("Syntax:")
_, issues = parse("events.json", '{"events": [}')
check_eq("invalid JSON -> error with line/column", issues[0].level == "error" and "line 1 column" in issues[0].message, True)
_, issues = parse("geofences.json", "﻿" + json.dumps(load("geofences.json")))
check_eq("UTF-8 BOM (e.g. Windows Notepad) is accepted", issues, [])

print("events.json:")
ev = load("events.json")
d = copy.deepcopy(ev); d["events"][0]["messages_match_type"] = "exact"
expect("'exact' -> use 'equals'", "events.json", d, "error", 'use "equals"')
d = copy.deepcopy(ev); d["events"][0]["messages_filtered_list"] = ["CAN9_GnssSpeed"]
expect("equals with a flat list -> list of lists", "events.json", d, "error", "list of lists")
d = copy.deepcopy(ev); d["events"][1]["messages_filtered_list"] = ["_DM01"]
expect("contains with a list -> string", "events.json", d, "error", "non-empty string")
d = copy.deepcopy(ev); del d["events"][0]["raster"]
expect("missing raster", "events.json", d, "error", "raster: missing")
d = copy.deepcopy(ev); d["events"][0]["raster"] = "1x"
expect("unknown raster unit", "events.json", d, "error", 'unknown unit "x"')
d = copy.deepcopy(ev); d["events"][0]["raster"] = "1S"
expect("older raster alias -> warning", "events.json", d, "warning", 'use "s"')
d = copy.deepcopy(ev); d["events"][0]["upper_threshold"] = "10"
expect("threshold as string", "events.json", d, "error", "must be a number")
d = copy.deepcopy(ev); d["events"][0]["lower_threshold"] = 20
expect("lower above upper -> warning", "events.json", d, "warning", "can never start")
d = copy.deepcopy(ev); d["events"][0]["raster"] = ""
expect("GPS without raster -> warning", "events.json", d, "warning", "GPS positions")
d = copy.deepcopy(ev); d["events"][0]["event_name"] = "a/b"
expect("event name with /", "events.json", d, "error", "must not contain /")

print("custom-messages.json:")
cm = load("custom-messages.json")
d = copy.deepcopy(cm); d[0]["custom_message_name"] = "CAN1-CALC DM01"
expect("invalid table name", "custom-messages.json", d, "error", "letters, digits and _")
d = copy.deepcopy(cm); d[1]["custom_message_name"] = d[0]["custom_message_name"]
expect("duplicate custom_message_name", "custom-messages.json", d, "error", "more than one entry")
d = copy.deepcopy(cm); d[0]["function"] = "my_function"
expect("custom function -> warning", "custom-messages.json", d, "warning", "not a built-in function")
d = copy.deepcopy(cm); d[0]["prefix"] = "false"
expect("prefix as string", "custom-messages.json", d, "error", "true or false")

print("geofences.json:")
d = load("geofences.json"); d[0]["latitude"] = 95
expect("latitude out of range", "geofences.json", d, "error", "-90 to 90")
d = load("geofences.json"); d[0]["radius"] = 0
expect("radius 0", "geofences.json", d, "error", "above 0")

print("aggregations.json:")
ag = load("aggregations.json")
d = copy.deepcopy(ag); del d["config"]["trip"]["trip_gap_min"]
expect("missing trip_gap_min", "aggregations.json", d, "error", "trip_gap_min: missing")
d = copy.deepcopy(ag); d["cluster_details"][0]["details"]["aggregations"][0]["aggregation"] = ["mean"]
expect("unknown aggregation method", "aggregations.json", d, "error", '"mean" is not supported')
d = copy.deepcopy(ag); d["config"]["date"]["end_date"] = "2022-05-01"
expect("end_date before start_date", "aggregations.json", d, "error", "is before start_date")
d = copy.deepcopy(ag); d["config"]["date"]["start_date"] = "2022-13-01"
expect("invalid date", "aggregations.json", d, "error", "YYYY-MM-DD")
d = copy.deepcopy(ag); d["device_clusters"][0]["devices"] = []
expect("empty device list", "aggregations.json", d, "error", "is empty")
d = copy.deepcopy(ag); d["device_clusters"][0]["cluster"] = "cluster2"
expect("cluster without cluster_details", "aggregations.json", d, "error", "no entry in cluster_details")
d = copy.deepcopy(ag); d["config"]["date"]["mode"] = "yesterday"
expect("unknown date mode", "aggregations.json", d, "error", "previous_day")

print("backlog.json:")
bl = load("backlog.json")
d = copy.deepcopy(bl); d["files"].append("2f6913db/")
expect("lower-case device -> read as upper case", "backlog.json", d, "warning", 'read as "2F6913DB/"')
d = copy.deepcopy(bl); d["files"].append("logs/2F6913DB")
expect("unknown entry -> ignored", "backlog.json", d, "warning", "is ignored")
d = copy.deepcopy(bl); d["config"]["batch_size"]["min"] = 300
expect("min above max", "backlog.json", d, "error", "is above max")
d = copy.deepcopy(bl); d["files"] = []
expect("no files", "backlog.json", d, "error", "nothing to process")
check_eq("classify device", classify_backlog_entry("2f6913db"), ("device", "2F6913DB/"))
check_eq("classify session", classify_backlog_entry("2F6913DB/00000001"), ("session", "2F6913DB/00000001/"))
check_eq("classify file", classify_backlog_entry("2f6913db/00000001/00000001.MF4"), ("file", "2F6913DB/00000001/00000001.MF4"))
check_eq("classify other", classify_backlog_entry("2F6913DB/abc/"), (None, "2F6913DB/abc/"))

print("dbc-groups.json:")
dg = load("dbc-groups.json")
d = copy.deepcopy(dg); d["dbc_groups"][0]["devices"].append("2f6913db")
expect("lower-case device", "dbc-groups.json", d, "error", "not a device ID")
d = copy.deepcopy(dg); d["dbc_groups"][0]["dbc_files"].append("CAN1-test.DBC")
expect("DBC name not lower case", "dbc-groups.json", d, "error", "can1-<name>.dbc")
d = copy.deepcopy(dg); d["dbc_groups"][0]["dbc_files"] += ["can11-router-s1.dbc", "can18-router2-s4.dbc"]
check_eq("CANmod.router channel prefixes (can11-, can18-) valid",
         [str(i) for i in validate("dbc-groups.json", d) if i.level == "error"], [])
d = copy.deepcopy(dg); d["dbc_groups"][0]["dbc_files"].append("can111-x.dbc")
expect("3-digit channel prefix rejected", "dbc-groups.json", d, "error", "can11-<name>.dbc")
d = copy.deepcopy(dg); d["dbc_groups"][1]["devices"].append("2F6913DB")
expect("device in two groups -> warning", "dbc-groups.json", d, "warning", "first group wins")
expect("missing dbc_groups", "dbc-groups.json", {"groups": []}, "error", "dbc_groups: missing")

print("routing.json:")
rt = load("routing.json")
d = copy.deepcopy(rt); d["config"]["mirror_to_default"] = "false"
expect('mirror_to_default "false" (string)', "routing.json", d, "error", "without quotes")
d = copy.deepcopy(rt); d["devices"]["2F6913DB"]["from_date"] = "May 2026"
expect("malformed from_date -> warning (default bucket)", "routing.json", d, "warning", "default output bucket")
expect("routing on Google -> warning", "routing.json", rt, "warning", "no effect on Google", cloud="Google")

print("check() raises on errors:")
try:
    check("events.json", '{"events": [{"event_name": "x"}]}', log)
    check_eq("ControlFileError raised", False, True)
except ControlFileError as e:
    check_eq("ControlFileError raised", e.name, "events.json")

print("Backlog batch grouping:")
from modules.utils import ProcessBacklog  # noqa: E402
pb = ProcessBacklog("Local", None, "bucket", log, min_batch_size=10, max_batch_size=256)
small = [[f"AAAAAAAA/{s:08d}/{f:08d}.MF4" for f in range(2)] for s in range(200)]   # 200 sessions x 2 files
big = [[f"BBBBBBBB/00000001/{f:08d}.MF4" for f in range(600)]]                         # 1 session x 600 files
batches = pb.group_batches(small + big)
check_eq("small sessions merged up to max", [len(b) for b in batches if b[0].startswith("AAAAAAAA")], [256, 144])
check_eq("large session split at max", [len(b) for b in batches if b[0].startswith("BBBBBBBB")], [256, 256, 88])
check_eq("never across devices", all(len({f.split('/')[0] for f in b}) == 1 for b in batches), True)
medium = [[f"CCCCCCCC/{s:08d}/{f:08d}.MF4" for f in range(12)] for s in range(3)]   # sessions >= min stay alone
check_eq("sessions >= min stay their own batch", [len(b) for b in pb.group_batches(medium)], [12, 12, 12])

print("Missing objects are 'not found' on every cloud:")


class NotFound(Exception): pass                     # google.api_core.exceptions.NotFound  # noqa: E701
class ResourceNotFoundError(Exception): pass        # azure.core.exceptions  # noqa: E701


class ClientError(Exception):                       # botocore.exceptions.ClientError
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


check_eq("Amazon 404", is_not_found_error(ClientError("404")), True)
check_eq("Amazon 403 is a real error", is_not_found_error(ClientError("403")), False)
check_eq("Google NotFound", is_not_found_error(NotFound()), True)
check_eq("Azure ResourceNotFoundError", is_not_found_error(ResourceNotFoundError()), True)
check_eq("Local FileNotFoundError", is_not_found_error(FileNotFoundError()), True)
check_eq("Other errors are real errors", is_not_found_error(PermissionError()), False)

print("Only publish_notification writes 'NEW EVENT':")
hits = []
for path in glob.glob(os.path.join(REPO, "modules", "*.py")):
    with open(path, encoding="utf-8") as f:
        for no, line in enumerate(f, 1):
            if "NEW EVENT" in line and not line.strip().startswith("#"):
                hits.append(f"{os.path.basename(path)}:{no}")
source = open(os.path.join(REPO, "modules", "cloud_functions.py"), encoding="utf-8").read()
start = source.index("def publish_notification")
end_match = re.search(r"\ndef ", source[start + 1:])
end = start + 1 + end_match.start() if end_match else len(source)
allowed ={f"cloud_functions.py:{source[:start + m.start()].count(chr(10)) + 1}" for m in re.finditer("NEW EVENT", source[start:end])}
check_eq("no other log line contains NEW EVENT", sorted(set(hits) - allowed), [])

print()
if _failures:
    print(f"{len(_failures)} FAILED: {_failures}")
    sys.exit(1)
print("All tests passed")
