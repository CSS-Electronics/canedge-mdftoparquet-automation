# Optional per-device output routing (routing.json) - an advanced feature for multi-tenant setups.
#
# routing.json lives in the input bucket root. It has a global "config" section and a "devices" map
# from device ID to a target output bucket plus a recording-date cutoff:
#
#   {
#     "config": {"mirror_to_default": false},
#     "devices": {
#       "2F6913DB": {"output_bucket": "myfleet-custA-parquet", "from_date": "2026-05-01"},
#       "7512BE4D": {"output_bucket": "myfleet-custB-parquet", "from_date": "2024-01-01"}
#     }
#   }
#
# Behaviour: if routing.json is absent/empty/invalid, or a device is not listed, or a rule is
# malformed, or a file's recording date precedes from_date, the data is written to the DEFAULT
# output bucket (the base deployment's "<input>-parquet"). That default bucket therefore acts as a
# visible catch-all for un-routed data (previous-owner stragglers, unmapped devices, config errors)
# - nothing is silently discarded.
#
# "mirror_to_default" (global, default false) controls whether routed devices are ALSO copied to the
# default bucket: false = route matched devices to their client bucket only; true = write matched
# devices to both their client bucket AND the default bucket, so in-house / OEM admins can see all
# devices in one lake (at the cost of duplicating that data). Catch-all files (no matching client
# bucket) are always written to the default bucket exactly once - mirroring never double-writes them.
#
# "from_date" is the RECORDING date (taken from the decoded Parquet path), not the upload date.
import re

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def resolve_mirror_to_default(routing_config):
    """Return the global "mirror_to_default" flag from routing.json (default False).

    When True, files for a routed (client) device are written to BOTH the client bucket and the
    default bucket; when False, routed devices go to their client bucket only. Robust to an
    absent/invalid config (download_json_file returns [] when routing.json is missing/invalid).
    """
    if not isinstance(routing_config, dict):
        return False
    config = routing_config.get("config")
    if not isinstance(config, dict):
        return False
    return bool(config.get("mirror_to_default", False))


def resolve_routing_rule(device_id, routing_config, logger):
    """Resolve the routing rule for this invocation's device.

    One cloud-function invocation decodes exactly one device (single-file upload, or a
    device-grouped backlog batch), so the rule is resolved once here and the per-file date
    cutoff is applied later in upload_files_to_cloud.

    Returns a validated {"output_bucket", "from_date"} dict, or None to mean "use the default
    output bucket" (which covers absent/empty/invalid routing.json and unmapped devices).
    """
    # download_json_file returns [] when the file is absent or invalid JSON
    if not isinstance(routing_config, dict) or not routing_config:
        return None

    devices = routing_config.get("devices")
    if not isinstance(devices, dict):
        return None

    rule = devices.get(device_id)
    if rule is None:
        logger.info(f"routing.json: device {device_id} not listed - using default output bucket (catch-all)")
        return None

    if not isinstance(rule, dict) or not rule.get("output_bucket"):
        logger.warning(f"routing.json: invalid rule for device {device_id} (missing 'output_bucket') - using default output bucket")
        return None

    from_date = str(rule.get("from_date", "") or "")
    if from_date and not _DATE_RE.match(from_date):
        # Don't guess a cutoff: fall back to the default bucket rather than risk leaking a
        # previous owner's data to the new customer.
        logger.warning(f"routing.json: device {device_id} has malformed 'from_date' '{from_date}' (expected YYYY-MM-DD) - using default output bucket")
        return None

    logger.info(f"routing.json: device {device_id} -> {rule['output_bucket']} (from_date={from_date or 'any'})")
    return {"output_bucket": rule["output_bucket"], "from_date": from_date}


def resolve_target_bucket(relative_path, routing_rule, default_bucket, logger):
    """Pick the output bucket for a single decoded file based on its recording date vs the rule.

    relative_path is '<device>/<message>/<yyyy>/<mm>/<dd>/<file>.parquet' for decoded/custom
    messages and 'aggregations/events/<yyyy>/<mm>/<dd>/<file>' for event tables - in both shapes
    the recording date is the three path segments immediately before the filename. Files dated
    >= from_date go to the rule's bucket; everything else falls back to the catch-all default.
    """
    if routing_rule is None:
        return default_bucket

    parts = relative_path.split("/")
    if len(parts) < 4:
        logger.warning(f"routing.json: cannot parse recording date from '{relative_path}' - using default output bucket")
        return default_bucket

    recording_date = f"{parts[-4]}-{parts[-3]}-{parts[-2]}"
    if not _DATE_RE.match(recording_date):
        logger.warning(f"routing.json: unexpected date segments in '{relative_path}' - using default output bucket")
        return default_bucket

    from_date = routing_rule["from_date"]
    if from_date and recording_date < from_date:
        # Recorded before this device's cutoff (e.g. a previous owner's residual data).
        return default_bucket

    return routing_rule["output_bucket"]


def resolve_target_buckets(relative_path, routing_rule, default_bucket, mirror_to_default, logger):
    """Return the ordered, de-duplicated list of buckets a single decoded file is written to.

    - Catch-all (no rule / date before from_date / unparseable path) -> [default_bucket] only, so a
      file that belongs in the default bucket is never written there twice.
    - Routed to a client bucket -> [client_bucket], plus default_bucket appended when
      mirror_to_default is True (mirror the file into the shared default lake as well).
    """
    routed = resolve_target_bucket(relative_path, routing_rule, default_bucket, logger)
    if routed == default_bucket:
        return [default_bucket]
    targets = [routed]
    if mirror_to_default:
        targets.append(default_bucket)
    return targets
