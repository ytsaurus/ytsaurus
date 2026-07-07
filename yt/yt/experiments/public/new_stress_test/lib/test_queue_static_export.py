from .logger import logger
from lib.schema import RandomStringGenerator

import yt.wrapper as yt

from yt.wrapper.retries import run_with_retries

from yt.common import YtError, wait

import logging
import os
import random
import time

from datetime import datetime, timezone

RSG = RandomStringGenerator()


# Stress test for Queue Agent static exports (queue -> static tables).
#
# Goal: make sure background exports performed by the Queue Agent do not lose,
# duplicate or reorder data. Meant to catch regressions when switching the Queue Agent
# between the old and the new export implementation (Controller/QueueExporter/
# Implementation = Old|New in the queue agent dynamic config) — that switch is a
# *deploy-side* setting, the test itself is agnostic to it.
#
# Design:
#   * Writes go through a queue *producer* (push_queue_producer), so every row gets a
#     unique, monotonic $sequence_number ("seq"); the producer dedups retried pushes, so
#     seq stays gap-free and the test is safe to restart.
#   * For every queue we keep a shadow *sorted* dynamic table that stores every row
#     written, keyed by (tablet, row_index). PushQueueProducer is an ITransaction method,
#     so the queue write and the shadow insert go into one tablet transaction and commit
#     atomically — the shadow is always an exact mirror of the queue.
#   * Exports: within one exported static table rows are ordered by
#     (tablet_index, row_index); across tables by time (default "%UNIX_TS-%PERIOD" name
#     pattern sorts chronologically). So, per tablet, concatenating exported rows in
#     table-name order must yield a contiguous prefix (row_index 0, 1, 2, ...).
#
# Persistence / resumability:
#   * All objects live at a FIXED state path (NOT the per-run directory the harness
#     hands us), and are created only if missing. So a fresh process attaches to the
#     existing queues and just keeps writing/verifying.
#   * Verification progress (per export, per tablet: next expected row_index, and the
#     last verified table) is persisted in a Cypress attribute on each export directory,
#     so we never re-read already verified export tables after a restart.
#
# We deliberately store tablet/row_index/seq as *real* payload columns so verification
# never depends on system columns ($tablet_index/$row_index) in the exported tables.


# Inline hunk threshold (bytes) for the "value" column of hunk-enabled queues. Values larger
# than this go to hunk chunks (in the linked hunk storage); kept well below write_min_row_size
# so essentially every written value becomes a hunk.
MAX_INLINE_HUNK_SIZE = 100

QUEUE_SCHEMA = [
    {"name": "tablet", "type": "int64"},
    {"name": "row_index", "type": "int64"},
    {"name": "seq", "type": "int64"},
    {"name": "value", "type": "string"},
]

SHADOW_SCHEMA = [
    {"name": "tablet", "type": "int64", "sort_order": "ascending"},
    {"name": "row_index", "type": "int64", "sort_order": "ascending"},
    {"name": "seq", "type": "int64"},
    {"name": "value", "type": "string"},
]

# Cypress attribute on each export directory holding our verification watermark.
VERIFY_STATE_ATTR = "stress_verify_state"


class ExportMismatchError(Exception):
    """An export genuinely disagrees with the shadow — real data loss/duplication/corruption.

    Deliberately NOT a YtError, so the run loop's `except YtError` (which tolerates transient
    infrastructure failures) does not swallow it: a real data discrepancy must always
    propagate and fail the run.
    """


class ExportStalenessError(Exception):
    """The Queue Agent has stopped creating exports for a queue (no new export table for too
    long). Like ExportMismatchError, NOT a YtError so it always propagates and fails the run —
    this is the headline regression we are watching for.
    """


class ExportVerifyStallError(Exception):
    """Verification itself is stuck: there are export tables we have not verified and the
    watermark has not advanced for too long, even though exports keep being created (so it is
    not a "no exports" problem). Catches the case where an export table is persistently
    unreadable and would otherwise be retried silently forever. NOT a YtError → fails the run.
    """


def _parse_yt_instant(value):
    # YT timestamps look like "2026-06-15T08:53:31.152989Z" (UTC). Return unix seconds.
    value = str(value).rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            pass
    raise ValueError(f"cannot parse YT instant {value!r}")


def _cron_interval_seconds(cron):
    # Best-effort sizing of a cron schedule: recognise the "0/N" / "*/N" step we use in the
    # seconds or minutes field of a 5- or 6-field cron. Returns None for anything we cannot
    # confidently size, in which case staleness monitoring is skipped for that export.
    if not cron:
        return None
    fields = cron.split()

    def step(field, unit):
        if field.startswith("0/") or field.startswith("*/"):
            try:
                return int(field.split("/")[1]) * unit
            except ValueError:
                return None
        return None

    # Take the smallest stepped field (seconds, then minutes, then hours).
    if len(fields) == 6:        # sec min hour dom mon dow
        units = list(zip(fields[:3], (1, 60, 3600)))
    elif len(fields) == 5:      # min hour dom mon dow
        units = list(zip(fields[:2], (60, 3600)))
    else:
        return None
    for field, unit in units:
        seconds = step(field, unit)
        if seconds is not None:
            return seconds
    return None


class Queue:
    def __init__(self, state_path, name, tablet_count, exports_cfg, erasure, hunks, commit_ordering,
                 auto_trim, flush_period_ms, export_ttl_ms, cron_default_interval_seconds, session_id):
        self.name = name
        self.path = f"{state_path}/{name}"
        self.shadow_path = f"{state_path}/{name}.shadow"
        self.producer_path = f"{state_path}/{name}.producer"
        self.hunk_storage_path = f"{state_path}/{name}.hunk_storage"
        self.tablet_count = tablet_count
        self.erasure = erasure
        self.hunks = hunks
        self.commit_ordering = commit_ordering
        self.auto_trim = auto_trim
        self.flush_period_ms = flush_period_ms
        self.export_ttl_ms = export_ttl_ms
        self.cron_default_interval = cron_default_interval_seconds
        self.session_id = session_id

        self.queue_id = None
        self.epoch = None
        # Next $sequence_number to assign (global, monotonic within the producer session).
        self.next_seq = 0
        # Per-tablet count of rows written so far (also the next row_index for the tablet).
        self.written_row_count = [0] * tablet_count
        # Watchdog state for the verification-stall guard, per export name: the last observed
        # watermark and the wall-clock time verification last advanced.
        self._verify_marker = {}
        self._verify_progress_time = {}

        # exports_cfg is a dict {export_name: export_cfg}. The export name (the key) is
        # used verbatim as the static_export_config key and to derive the export directory,
        # so names are stable across config edits (unlike list indices). Each export_cfg is
        #   {"period": <seconds>} OR {"cron": "<cron expr>"}  (schedule, mutually exclusive)
        #   optional: "name_pattern", "use_upper_bound" (bool)
        # A single large export_ttl_ms (e.g. ~2 weeks) is applied to every export so tables
        # do not pile up forever. It is intentionally global, not per-export: the TTL must
        # be >> the verify cadence, otherwise a table could expire before the verifier reads
        # it and look like a row_index gap (false data-loss failure).
        self.exports = {}
        for export_name, export_cfg in exports_cfg.items():
            directory = f"{state_path}/{name}.{export_name}"
            self.exports[export_name] = {
                "directory": directory,
                "config_entry": self._build_export_config_entry(directory, export_cfg, export_ttl_ms),
                "label": self._export_label(export_cfg),
                "interval": self._export_interval_seconds(export_cfg),
            }

    def _export_interval_seconds(self, export_cfg):
        # Expected gap between consecutive export tables, used for the staleness alert.
        # period -> the period; cron -> best-effort parse; if a cron cannot be sized we fall
        # back to cron_default_interval (with a warning) rather than silently not monitoring
        # it — set "expected_interval_seconds" on the export to pin it exactly.
        if "expected_interval_seconds" in export_cfg:
            return export_cfg["expected_interval_seconds"]
        if "period" in export_cfg:
            return export_cfg["period"]
        cron = export_cfg.get("cron")
        parsed = _cron_interval_seconds(cron)
        if parsed is not None:
            return parsed
        if self.cron_default_interval:
            logger.warning(
                f"Queue {self.name}: cannot size cron {cron!r} for the staleness alert; using "
                f"default {self.cron_default_interval}s (set 'expected_interval_seconds' to tune)")
            return self.cron_default_interval
        return None

    def _build_export_config_entry(self, directory, export_cfg, export_ttl_ms):
        entry = {"export_directory": directory}
        if "cron" in export_cfg:
            entry["export_cron_schedule"] = export_cfg["cron"]
        else:
            entry["export_period"] = export_cfg["period"] * 1000
        if export_ttl_ms:
            entry["export_ttl"] = export_ttl_ms
        if "name_pattern" in export_cfg:
            entry["output_table_name_pattern"] = export_cfg["name_pattern"]
        if "use_upper_bound" in export_cfg:
            entry["use_upper_bound_for_table_names"] = export_cfg["use_upper_bound"]
        if self.hunks:
            # The exporter refuses to export a queue whose schema has hunk columns unless this
            # is set explicitly (queue_exporter.cpp).
            entry["enable_export_from_queue_with_hunks"] = True
        return entry

    @staticmethod
    def _export_label(export_cfg):
        if "cron" in export_cfg:
            return f"cron:{export_cfg['cron']}"
        return f"period:{export_cfg['period']}s"

    # -- setup ----------------------------------------------------------------

    def setup(self):
        self._ensure_queue()
        self._ensure_unfrozen()
        self._ensure_shadow()
        self._ensure_producer()
        self._apply_auto_trim()
        self._ensure_exports()

        # Resume writer state: seq from the producer session, per-tablet row counts from
        # the queue itself (total_row_count is the next $row_index for an ordered table).
        session = yt.create_queue_producer_session(self.producer_path, self.path, self.session_id)
        self.epoch = int(session["epoch"])
        committed_seq = int(session["sequence_number"])
        self.next_seq = committed_seq + 1

        tablet_infos = yt.get_tablet_infos(self.path, list(range(self.tablet_count)))["tablets"]
        self.written_row_count = [int(info["total_row_count"]) for info in tablet_infos]

        logger.info(
            f"Attached to queue {self.path} (epoch: {self.epoch}, next_seq: {self.next_seq}, "
            f"written_row_count: {self.written_row_count})")

    def _ensure_queue(self):
        if yt.exists(self.path):
            # The queue already exists (we are resuming). Its structural properties cannot
            # be changed in place, so make sure they still match the config — otherwise the
            # config was edited in a way that silently disagrees with the live object.
            self._verify_queue_matches_config()
            if self.hunks:
                self._ensure_hunk_storage()
            return
        logger.info(
            f"Creating queue {self.path} (tablet_count: {self.tablet_count}, erasure: {self.erasure}, "
            f"hunks: {self.hunks}, commit_ordering: {self.commit_ordering}, "
            f"flush_period_ms: {self.flush_period_ms})")
        # Copy the schema so the per-queue hunk tweak does not mutate the module constant.
        schema = [dict(column) for column in QUEUE_SCHEMA]
        if self.hunks:
            for column in schema:
                if column["name"] == "value":
                    column["max_inline_hunk_size"] = MAX_INLINE_HUNK_SIZE
        attributes = {
            "dynamic": True,
            "enable_dynamic_store_read": True,
            "schema": schema,
            "tablet_count": self.tablet_count,
            # Flush dynamic stores into chunks frequently so the Queue Agent has fresh
            # chunks to export (exports never see unflushed data; default flush is 15m).
            "mount_config": {"dynamic_store_auto_flush_period": self.flush_period_ms},
        }
        if self.erasure:
            attributes["erasure_codec"] = "isa_reed_solomon_6_3"
        if self.commit_ordering is not None:
            attributes["commit_ordering"] = self.commit_ordering
        yt.create("table", self.path, attributes=attributes)
        # Link the hunk storage while the queue is still unmounted (so no remount is needed),
        # then mount.
        if self.hunks:
            self._ensure_hunk_storage()
        yt.mount_table(self.path, sync=True)

    def _ensure_hunk_storage(self):
        # Create (if missing), mount and link the queue's hunk storage. Idempotent: on resume
        # the storage exists and the queue is already linked, so this only ensures it is
        # mounted. A queue with hunk columns needs a linked hunk storage to write/flush hunks.
        if not yt.exists(self.hunk_storage_path):
            attributes = {
                "tablet_count": 1,
                # Rotate hunk stores into chunks reasonably fast so exported chunks reference
                # sealed hunks promptly (default rotation is geared to 15m flushes).
                "store_rotation_period": 10000,
            }
            cell_tag = self._queue_external_cell_tag()
            if cell_tag is not None:
                attributes["external_cell_tag"] = cell_tag
            logger.info(f"Creating hunk storage {self.hunk_storage_path} (cell_tag: {cell_tag})")
            yt.create("hunk_storage", self.hunk_storage_path, attributes=attributes)
        if yt.get(f"{self.hunk_storage_path}/@tablet_state") != "mounted":
            yt.mount_table(self.hunk_storage_path, sync=True)
        if not yt.exists(f"{self.path}/@hunk_storage_id"):
            hunk_storage_id = yt.get(f"{self.hunk_storage_path}/@id")
            logger.info(f"Linking hunk storage {self.hunk_storage_path} to queue {self.path}")
            yt.set(f"{self.path}/@hunk_storage_id", hunk_storage_id)
            if yt.get(f"{self.path}/@tablet_state") == "mounted":
                yt.remount_table(self.path)

    def _queue_external_cell_tag(self):
        if yt.exists(f"{self.path}/@external_cell_tag"):
            return yt.get(f"{self.path}/@external_cell_tag")
        return None

    def _ensure_unfrozen(self):
        # flush() does freeze->unfreeze; if a previous run died between the two steps
        # (e.g. the cluster was unavailable and the process crashed/was killed), the queue
        # is left "frozen" and silently rejects all writes forever. Recover here.
        state = yt.get(f"{self.path}/@tablet_state")
        if state in ("frozen", "freezing"):
            logger.info(f"Queue {self.path} is '{state}'; unfreezing to recover")
            yt.unfreeze_table(self.path, sync=True)

    def _verify_queue_matches_config(self):
        # Compare the live queue against the config for the structural attributes that
        # cannot be reconciled by a simple set() (tablet_count needs reshard, erasure /
        # commit_ordering / schema need an unmounted alter). On mismatch we fail loudly
        # rather than running against an object that does not match its name's config.
        mismatches = []

        actual_tablet_count = yt.get(f"{self.path}/@tablet_count")
        if actual_tablet_count != self.tablet_count:
            mismatches.append(f"tablet_count: config={self.tablet_count}, actual={actual_tablet_count}")

        actual_erasure = yt.get(f"{self.path}/@erasure_codec")
        expected_erasure = "isa_reed_solomon_6_3" if self.erasure else "none"
        if actual_erasure != expected_erasure:
            mismatches.append(f"erasure_codec: config={expected_erasure}, actual={actual_erasure}")

        # commit_ordering: only enforce what the config explicitly pins (otherwise accept
        # whatever default the queue was created with).
        if self.commit_ordering is not None:
            actual_commit_ordering = yt.get(f"{self.path}/@commit_ordering")
            if actual_commit_ordering != self.commit_ordering:
                mismatches.append(
                    f"commit_ordering: config={self.commit_ordering}, actual={actual_commit_ordering}")

        # hunks change the schema (max_inline_hunk_size) and require a linked hunk storage —
        # neither can be toggled in place, so the link's presence must match the config.
        actual_hunks = yt.exists(f"{self.path}/@hunk_storage_id")
        if actual_hunks != bool(self.hunks):
            mismatches.append(f"hunks: config={bool(self.hunks)}, actual(linked hunk_storage)={actual_hunks}")

        if mismatches:
            raise YtError(
                f"Existing queue {self.path} does not match its configuration: "
                f"{'; '.join(mismatches)}. These are structural and cannot be changed in place — "
                f"give the queue a new name (key) in the config, or remove the old object.")

    def _apply_auto_trim(self):
        # auto_trim_config is mutable, so (re)apply it on every startup to keep the config
        # the source of truth. Trim is safe for verification: it touches the live queue,
        # not the shadow or the export tables, and total_row_count (our row_index counter)
        # is unaffected. NB: real trimming also needs Controller/EnableAutomaticTrimming on
        # in the queue agent dynamic config (deploy-side).
        yt.set(f"{self.path}/@auto_trim_config", {"enable": self.auto_trim})

    def _ensure_shadow(self):
        if not yt.exists(self.shadow_path):
            logger.info(f"Creating shadow table {self.shadow_path}")
            yt.create("table", self.shadow_path, attributes={
                "dynamic": True,
                "enable_dynamic_store_read": True,
                "schema": SHADOW_SCHEMA,
            })
        yt.mount_table(self.shadow_path, sync=True)

    def _ensure_producer(self):
        if not yt.exists(self.producer_path):
            logger.info(f"Creating queue producer {self.producer_path}")
            yt.create("queue_producer", self.producer_path)
        # create queue_producer auto-mounts; make sure it is ready before use.
        wait(lambda: yt.get(f"{self.producer_path}/@tablet_state") == "mounted",
             error_message=f"Producer {self.producer_path} did not mount")

    def _ensure_exports(self):
        self.queue_id = yt.get(f"{self.path}/@id")
        for export_name, export in self.exports.items():
            directory = export["directory"]
            if not yt.exists(directory):
                logger.info(
                    f"Creating export destination {directory} "
                    f"(export: {export_name}, {export['label']})")
                yt.create("map_node", directory)
            yt.set(f"{directory}/@queue_static_export_destination", {
                "originating_queue_id": self.queue_id,
            })

        static_export_config = {
            export_name: export["config_entry"]
            for export_name, export in self.exports.items()
        }
        logger.info(f"Setting static_export_config on {self.path}: {static_export_config}")
        yt.set(f"{self.path}/@static_export_config", static_export_config)

    # -- writing --------------------------------------------------------------

    def write(self, spec):
        cfg = spec.queue_static_export
        batch_size = random.randint(cfg.write_min_batch_size, cfg.write_max_batch_size)

        queue_rows = []
        shadow_rows = []
        running_count = {}
        for index in range(batch_size):
            tablet = random.randint(0, self.tablet_count - 1)
            row_index = self.written_row_count[tablet] + running_count.get(tablet, 0)
            running_count[tablet] = running_count.get(tablet, 0) + 1
            seq = self.next_seq + index
            value = RSG.generate(random.randint(cfg.write_min_row_size, cfg.write_max_row_size))
            queue_rows.append({
                "$tablet_index": tablet, "$sequence_number": seq,
                "tablet": tablet, "row_index": row_index, "seq": seq, "value": value,
            })
            shadow_rows.append({
                "tablet": tablet, "row_index": row_index, "seq": seq, "value": value,
            })

        logger.info(f"Pushing {batch_size} rows to queue {self.path} (first seq: {self.next_seq})")

        # PushQueueProducer is an ITransaction method: it writes the queue rows and
        # advances the producer's sequence_number as part of the ambient transaction. So
        # the queue write and the shadow insert go into a single tablet transaction and
        # commit atomically — the shadow is always an exact mirror of the queue, no
        # restart-time reconciliation needed. Producer dedup by $sequence_number keeps a
        # retried transaction idempotent.
        def _write_tx():
            with yt.Transaction(type="tablet"):
                yt.push_queue_producer(
                    self.producer_path, self.path, self.session_id, self.epoch, queue_rows)
                yt.insert_rows(self.shadow_path, shadow_rows)

        # Retry a bounded number of times to ride out brief tablet conflicts / blips; if the
        # write still fails after that, the exception propagates and the run fails — a
        # persistently unwritable queue is a real problem we must surface, not retry on for
        # hours. Producer dedup keeps retries idempotent.
        run_with_retries(
            _write_tx, retry_count=cfg.write_retry_count, backoff=0.1,
            backoff_config={"policy": "constant_time", "constant_time": 0.1},
            except_action=lambda ex: logger.error(f"Write transaction failed, retrying: {ex.simplify()}"))

        self.next_seq += batch_size
        for tablet, count in running_count.items():
            self.written_row_count[tablet] += count

    def flush(self):
        logger.info(f"Flushing queue {self.path}")
        yt.freeze_table(self.path, sync=True)
        yt.unfreeze_table(self.path, sync=True)

    # -- verification ---------------------------------------------------------

    def verify_exports(self):
        for export_name, export in self.exports.items():
            self._verify_export(export_name, export)

    def _load_verify_state(self, directory):
        if yt.exists(f"{directory}/@{VERIFY_STATE_ATTR}"):
            state = yt.get(f"{directory}/@{VERIFY_STATE_ATTR}")
            return (list(state["next_row_index"]),
                    (state.get("last_creation_time", ""), state.get("last_table", "")))
        return [0] * self.tablet_count, ("", "")

    def _store_verify_state(self, directory, next_row_index, last_key):
        yt.set(f"{directory}/@{VERIFY_STATE_ATTR}", {
            "next_row_index": next_row_index,
            "last_creation_time": last_key[0],
            "last_table": last_key[1],
        })

    def _verify_export(self, export_name, export):
        directory = export["directory"]
        next_row_index, last_key = self._load_verify_state(directory)

        # Export tables are immutable once visible. We order them by @creation_time (NOT
        # by name): with cron schedules or a custom output_table_name_pattern the name is
        # not necessarily chronological, but the Queue Agent creates the tables strictly
        # sequentially, so creation_time is a robust order. The watermark is the
        # (creation_time, name) of the last verified table; process only newer ones,
        # continuing each tablet's row_index stream where we left off.
        entries = sorted(
            (str(item.attributes["creation_time"]), str(item))
            for item in yt.list(directory, attributes=["creation_time"]))
        new_tables = [(ct, name) for ct, name in entries if (ct, name) > last_key]
        if not new_tables:
            return

        logger.info(
            f"Verifying {len(new_tables)} new export table(s) in {directory} (export: {export_name})")

        for creation_time, table_name in new_tables:
            self._verify_export_table(directory, table_name, next_row_index)
            last_key = (creation_time, table_name)
            self._store_verify_state(directory, next_row_index, last_key)

    def _verify_export_table(self, directory, table_name, next_row_index):
        table_path = f"{directory}/{table_name}"
        rows = list(yt.read_table(table_path))

        # Rows in one export table arrive ordered by (tablet, row_index).
        by_tablet = {}
        for row in rows:
            by_tablet.setdefault(row["tablet"], []).append(row)

        for tablet, tablet_rows in by_tablet.items():
            expected_first = next_row_index[tablet]

            # No gaps and no duplicates: the exported row_index stream for this tablet
            # must be exactly expected_first, expected_first + 1, ...
            for offset, row in enumerate(tablet_rows):
                expected_row_index = expected_first + offset
                if row["row_index"] != expected_row_index:
                    raise ExportMismatchError(
                        f"Export {table_path}: tablet {tablet} row_index stream broken: "
                        f"expected {expected_row_index} but got {row['row_index']} "
                        f"(seq: {row['seq']}). Possible data loss/duplication in export.")

            last_row_index = expected_first + len(tablet_rows) - 1
            self._check_against_shadow(table_path, tablet, expected_first, last_row_index, tablet_rows)
            next_row_index[tablet] = last_row_index + 1

    def _check_against_shadow(self, table_path, tablet, lo, hi, exported_rows):
        expected = list(yt.select_rows(
            f"tablet, row_index, seq, value from [{self.shadow_path}] "
            f"where tablet = {tablet} and row_index >= {lo} and row_index <= {hi} "
            f"order by tablet, row_index limit {hi - lo + 1}"))

        if len(expected) != len(exported_rows):
            raise ExportMismatchError(
                f"Export {table_path}: tablet {tablet} exported {len(exported_rows)} rows for "
                f"row_index [{lo}, {hi}] but shadow {self.shadow_path} has {len(expected)} "
                f"(shadow behind export — possible data loss in export).")

        for exported_row, expected_row in zip(exported_rows, expected):
            for column in ("row_index", "seq", "value"):
                if exported_row[column] != expected_row[column]:
                    raise ExportMismatchError(
                        f"Export {table_path}: tablet {tablet} row_index {expected_row['row_index']} "
                        f"mismatch in '{column}': exported {exported_row[column]!r} but shadow has "
                        f"{expected_row[column]!r}. Export corrupted data.")

    def log_lag(self):
        written = sum(self.written_row_count)
        for export_name, export in self.exports.items():
            next_row_index, _ = self._load_verify_state(export["directory"])
            exported = sum(next_row_index)
            logger.info(
                f"Queue {self.path} export {export_name} ({export['label']}): "
                f"verified {exported} / written {written} rows (lag {written - exported})")

    def check_staleness(self, now, factor, slack):
        # Alert if the Queue Agent stopped producing exports: the newest export table must be
        # no older than interval*factor + slack. We write continuously, so for a period-P
        # export a healthy agent creates a table roughly every P; a much larger gap means the
        # exporter is stalled. The age comes from the export tables' @creation_time (so it is
        # persistent and survives restarts); if the directory has no tables yet we measure
        # from when the directory itself was created.
        if not factor:
            return
        for export in self.exports.values():
            interval = export["interval"]
            if not interval:
                continue  # cron we could not size / monitoring disabled for this export
            directory = export["directory"]
            entries = list(yt.list(directory, attributes=["creation_time"]))
            if entries:
                newest = max(_parse_yt_instant(e.attributes["creation_time"]) for e in entries)
            else:
                newest = _parse_yt_instant(yt.get(f"{directory}/@creation_time"))
            age = now - newest
            threshold = interval * factor + slack
            if age > threshold:
                raise ExportStalenessError(
                    f"Export {directory} ({export['label']}): no new export table for {int(age)}s "
                    f"(threshold {int(threshold)}s = {factor}x{interval}s + {slack}s) — the Queue "
                    f"Agent appears to have stopped creating exports.")

    def check_verify_progress(self, now, threshold):
        # Watchdog for stuck verification: if there are export tables newer than our watermark
        # (pending work) but the watermark has not advanced for longer than `threshold`, fail.
        # This catches a persistently unreadable export table, which the verify loop would
        # otherwise swallow as an "infra" error and retry forever (silently). The timer resets
        # whenever the watermark advances, so a slow-but-progressing verify never trips it.
        # Reads here are cheap metadata (list + attribute) that succeed even when a table's
        # data chunks are unreadable, so the watchdog can fire while verify itself cannot read.
        if not threshold:
            return
        for export_name, export in self.exports.items():
            directory = export["directory"]
            _, marker = self._load_verify_state(directory)
            entries = [(str(item.attributes["creation_time"]), str(item))
                       for item in yt.list(directory, attributes=["creation_time"])]
            newest = max(entries) if entries else None
            pending = newest is not None and newest > marker

            if export_name not in self._verify_marker or marker != self._verify_marker[export_name]:
                # First observation, or verification advanced — record progress.
                self._verify_marker[export_name] = marker
                self._verify_progress_time[export_name] = now
                continue

            stalled = now - self._verify_progress_time[export_name]
            if pending and stalled > threshold:
                raise ExportVerifyStallError(
                    f"Export {directory} ({export['label']}): verification stuck — newest export "
                    f"table {newest[1]} is unverified and the watermark has not advanced for "
                    f"{int(stalled)}s (> {threshold}s) while exports keep being created "
                    f"(an export table may be unreadable).")


def test_queue_static_export(base_path, spec, attributes, args):
    logging.getLogger('Yt').setLevel(logging.DEBUG)

    yt.config["backend"] = "rpc"
    yt.config["driver_config"] = {"enable_retries": True}
    yt.config["dynamic_table_retries"]["backoff"] = {"policy": "constant_time", "constant_time": 0.1}
    yt.config["dynamic_table_retries"]["total_timeout"] = 180000
    yt.config["tablets_ready_timeout"] = 4 * 60 * 1000

    cfg = spec.queue_static_export

    # Persistent objects live at a FIXED path, independent of the per-run directory the
    # harness creates, so restarts attach to the existing queues instead of recreating.
    state_path = cfg.state_path
    if not state_path:
        state_path = base_path.rsplit("/", 1)[0] + "/queue_static_export_state"
    logger.info(f"Using persistent state path {state_path}")
    yt.create("map_node", state_path, recursive=True, ignore_existing=True)

    # Each pod owns its own queues (namespaced by its DC), so every queue/producer has
    # exactly one writer and the (tablet, row_index)-keyed verification stays valid. Pods
    # share the config and the state path, but operate on disjoint objects. Assumes one
    # pod per DC. Since the producer is already per-pod, the producer session id can stay
    # a plain constant — no need to disambiguate it by DC.
    pod_dc = os.environ.get("DEPLOY_NODE_DC")
    pod_suffix = f"_{pod_dc}" if pod_dc else ""
    session_id = cfg.session_id
    logger.info(f"Pod DC: {pod_dc}, object suffix: '{pod_suffix}', producer session id: {session_id}")

    # cfg.queues is a dict {queue_name: queue_cfg} of named combinations to cover at once.
    # The name (key) is used verbatim as the object name (stable across config edits — no
    # index remapping). Each queue_cfg:
    #   {"tablet_count": <int>, "exports": {<export_name>: <export cfg>, ...},
    #    optional: "erasure" bool, "commit_ordering" "weak"|"strong",
    #              "flush_period_ms" int (overrides the global default), "auto_trim" bool}
    # where each export cfg is {"period": <s>} or {"cron": "<expr>"} (+ optional
    # name_pattern / use_upper_bound).
    queues = []
    for queue_name, queue_cfg in cfg.queues.to_dict().items():
        if not queue_cfg.get("enable", True):
            logger.info(f"Skipping disabled queue combination '{queue_name}'")
            continue
        queue = Queue(
            state_path, f"{queue_name}{pod_suffix}",
            tablet_count=queue_cfg["tablet_count"],
            exports_cfg=queue_cfg["exports"],
            erasure=queue_cfg.get("erasure", False),
            hunks=queue_cfg.get("hunks", False),
            commit_ordering=queue_cfg.get("commit_ordering"),
            auto_trim=queue_cfg.get("auto_trim", False),
            flush_period_ms=queue_cfg.get("flush_period_ms", cfg.flush_period_ms),
            export_ttl_ms=(cfg.export_ttl_seconds * 1000) if cfg.export_ttl_seconds else None,
            cron_default_interval_seconds=cfg.cron_default_interval_seconds,
            session_id=session_id)
        queue.setup()
        queues.append(queue)

    deadline = None
    if cfg.max_run_duration_seconds:
        deadline = time.time() + cfg.max_run_duration_seconds

    def _time_left():
        return deadline is None or time.time() < deadline

    iteration = 0
    last_verify = 0.0
    while _time_left() and (deadline is not None or iteration < spec.size.iterations):
        logger.iteration = iteration

        for queue in queues:
            # write() retries transient tablet errors cfg.write_retry_count times; if it
            # still fails, the exception propagates and the run FAILS — a queue we cannot
            # write to is a real problem we must surface (do NOT swallow it). flush() is
            # best-effort (auto-flush already produces chunks), so its failure is tolerated;
            # any frozen leftover is recovered by _ensure_unfrozen on the next startup.
            queue.write(spec)
            if random.random() < cfg.flush_probability:
                try:
                    queue.flush()
                except YtError as err:
                    logger.error(f"flush failed for {queue.path} (best-effort, ignoring): {err}")

        if time.time() - last_verify > cfg.verify_period_seconds:
            for queue in queues:
                try:
                    queue.verify_exports()
                    queue.check_staleness(
                        time.time(), cfg.export_staleness_factor, cfg.export_staleness_slack_seconds)
                except YtError as err:
                    # Transient infrastructure error while reading exports/shadow (e.g. a
                    # cell blip) — tolerate it; the watermark is persisted, so we resume
                    # cleanly next sweep. Real problems raise ExportMismatchError /
                    # ExportStalenessError (not YtErrors), which are NOT caught here and
                    # fail the run.
                    logger.error(f"verify failed for {queue.path} (infra), will retry: {err}")

                # Verification-stall watchdog: runs regardless of the outcome above, because a
                # persistently FAILING verify (e.g. an unreadable export table) is exactly what
                # it catches. Its own reads are cheap metadata, so tolerate their transient
                # failures; a real stall raises ExportVerifyStallError (not a YtError).
                try:
                    queue.check_verify_progress(time.time(), cfg.verify_stall_seconds)
                    queue.log_lag()
                except YtError as err:
                    logger.error(f"verify-progress/log for {queue.path} (infra): {err}")
            last_verify = time.time()

        time.sleep(cfg.iteration_sleep_seconds)
        iteration += 1

    # Final verification sweep of whatever has been exported by now.
    logger.iteration = None
    logger.info("Final export verification sweep")
    for queue in queues:
        queue.verify_exports()
        queue.log_lag()
