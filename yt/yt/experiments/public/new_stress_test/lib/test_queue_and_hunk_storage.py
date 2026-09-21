from .logger import logger

import yt.wrapper as yt

from yt.wrapper.retries import run_with_retries

from yt.common import YtError, wait
from lib.schema import RandomStringGenerator

from contextlib import contextmanager
import copy
import logging
import random


RSG = RandomStringGenerator()


def _get_external_cell_tag(path):
    return yt.get(f"{path}/@", attributes=["external_cell_tag"]).get("external_cell_tag")


def simple_mapper(input_row):
    yield {"key": input_row["key"], "value": input_row["value"]}


def simple_reducer(key, input_row_iterator):
    for input_row in input_row_iterator:
        yield {"key": key["key"], "value": input_row["value"]}


class MountState:
    def __init__(self, tablet_count):
        self.tablet_count = tablet_count
        self.is_mounted_tablet = [False] * tablet_count
        self.is_sync = [True] * tablet_count

    def _is_relevant_tablet(self, tablet_index, is_mount, sync):
        result = True
        if is_mount:
            result = result and self.is_mounted_tablet[tablet_index]
        else:
            result = result and not self.is_mounted_tablet[tablet_index]

        if sync:
            result = result and self.is_sync[tablet_index]
        else:
            result = result and not self.is_sync[tablet_index]
        return result

    def has_mounted_tablet(self, sync=None):
        if sync is None:
            return True in self.is_mounted_tablet
        else:
            return True in [self._is_relevant_tablet(tablet_index, True, sync) for tablet_index in range(self.tablet_count)]

    def has_unmounted_tablet(self, sync=None):
        if sync is None:
            return False in self.is_mounted_tablet
        else:
            return True in [self._is_relevant_tablet(tablet_index, False, sync) for tablet_index in range(self.tablet_count)]

    def get_mounted_tablet_indexes(self, tablet_index, sync):
        return self._get_mounted_tablet_indexes_impl(True, tablet_index, sync)

    def get_unmounted_tablet_indexes(self, tablet_index, sync):
        return self._get_mounted_tablet_indexes_impl(False, tablet_index, sync)

    def _get_mounted_tablet_indexes_impl(self, is_mount, tablet_index, sync):
        tablets = [tablet_index] if tablet_index is not None else list(range(self.tablet_count))
        return [tablet_index for tablet_index in tablets if self._is_relevant_tablet(tablet_index, is_mount, sync)]

    def mount(self, tablet_index, sync=True):
        self._mount_impl(True, tablet_index, sync)

    def unmount(self, tablet_index, sync=True):
        self._mount_impl(False, tablet_index, sync)

    def _mount_impl(self, is_mount, tablet_index, sync):
        if not tablet_index is not None:
            self.is_mounted_tablet = [is_mount] * self.tablet_count
            self.is_sync = [sync] * self.tablet_count
        else:
            self.is_mounted_tablet[tablet_index] = is_mount
            self.is_sync[tablet_index] = sync


def wait_for_pending_mounts(obj, tablet_index):
    pending_tablet_indexes = obj.mount_state.get_mounted_tablet_indexes(tablet_index, sync=False)
    if pending_tablet_indexes:
        logger.info(f"Waiting for tablets {pending_tablet_indexes} of {obj.path} to mount")
        wait_for_tablet_state(obj.path, pending_tablet_indexes, "mounted")
        for pending_tablet_index in pending_tablet_indexes:
            obj.mount_state.mount(pending_tablet_index)


def wait_for_pending_unmounts(obj, tablet_index):
    pending_tablet_indexes = obj.mount_state.get_unmounted_tablet_indexes(tablet_index, sync=False)
    if pending_tablet_indexes:
        logger.info(f"Waiting for tablets {pending_tablet_indexes} of {obj.path} to unmount")
        obj.wait_for_unmount(pending_tablet_indexes)


def wait_for_tablet_condition(predicate, error_message):
    wait(
        predicate,
        error_message=error_message,
        timeout=yt.config["tablets_ready_timeout"] / 1000,
        sleep_backoff=yt.config["tablets_check_interval"] / 1000,
    )


def wait_for_tablet_state(path, tablet_indexes, state):
    def _tablets_ready():
        tablets = yt.get(f"{path}/@tablets")
        return all(
            tablets[tablet_index]["state"] == state
            for tablet_index in tablet_indexes
        )

    wait_for_tablet_condition(
        _tablets_ready,
        error_message=f"Tablets of {path} did not become {state}",
    )


@contextmanager
def sync_unmount_queue_temporarily(queue):
    # These flags describe target states, including mounts/unmounts still in progress.
    mounted_tablet_indexes = [
        index for index, mounted in enumerate(queue.mount_state.is_mounted_tablet) if mounted
    ]
    if mounted_tablet_indexes or queue.mount_state.has_unmounted_tablet(sync=False):
        queue.unmount()
    try:
        yield
    finally:
        for tablet_index in mounted_tablet_indexes:
            queue.mount(tablet_index=tablet_index, sync=False)
        wait_for_pending_mounts(queue, tablet_index=None)


def wait_for_hunk_storage_unmounts(tablets_by_storage):
    pending = {storage: indexes for storage, indexes in tablets_by_storage.items() if indexes}
    if not pending:
        return

    def _unmounted():
        for storage, tablet_indexes in list(pending.items()):
            tablets = yt.get(f"{storage.path}/@tablets")
            tablets = [tablets[index] for index in tablet_indexes]
            storage._async_unmount_locking_queue_tablets(tablets)
            if all(tablet["state"] == "unmounted" for tablet in tablets):
                for tablet_index in tablet_indexes:
                    storage.mount_state.unmount(tablet_index)
                del pending[storage]
        return not pending

    paths = ", ".join(storage.path for storage in pending)
    # Locks may appear after the first orchid snapshot, before unmount reaches the node.
    wait_for_tablet_condition(
        _unmounted,
        error_message=f"Tablets of hunk storages {paths} did not become unmounted",
    )


# Inline hunk threshold for the queue's value column. Values larger than this are
# stored in the linked hunk storage; smaller ones stay inline. Kept as a single
# constant so every schema that should produce hunks agrees on the value.
MAX_INLINE_HUNK_SIZE = 512
DATA_TABLE_WRITE_BATCH_SIZE = 20_000

HUNK_STORAGE_ERASURE_ATTRIBUTES = {
    "erasure_codec": "reed_solomon_3_3",
    "replication_factor": 1,
    "read_quorum": 4,
    "write_quorum": 5,
}

QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "value", "type": "string", "max_inline_hunk_size": MAX_INLINE_HUNK_SIZE},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

# Same columns as QUEUE_SCHEMA without max_inline_hunk_size. Used for replication
# sources and replicas without hunks, exercising replication between both schemas.
QUEUE_SCHEMA_NO_HUNKS = [
    {"name": "key", "type": "string"},
    {"name": "value", "type": "string"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

# Schema for queues created via alter_to_queue: no $cumulative_data_weight,
# so altering an existing static table (which has key/value only) is a pure schema
# relaxation. Static results are pre-created with strict=true so this alter is valid
# (dynamic tables require strict=true, and alter cannot tighten strict).
ALTERED_QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "value", "type": "string", "max_inline_hunk_size": MAX_INLINE_HUNK_SIZE},
]

# Schema applied to the result of single-source operations that produce sorted output
# (sort, map_reduce-by-key). Pre-creating with a strict schema avoids YT's default
# strict=false for op outputs on dynamic inputs.
SORTED_KV_SCHEMA = [
    {"name": "key", "type": "string", "sort_order": "ascending"},
    {"name": "value", "type": "string", "sort_order": "ascending"},
]

KEY_SORTED_KV_SCHEMA = [
    {"name": "key", "type": "string", "sort_order": "ascending"},
    {"name": "value", "type": "string"},
]

UNSORTED_KV_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "value", "type": "string"},
]

QUEUE_DATA_SCHEMA = [
    {"name": "tablet_index", "type": "int64", "sort_order": "ascending"},
    {"name": "row_index", "type": "int64", "sort_order": "ascending"},
    {"name": "key", "type": "string"},
    {"name": "value", "type": "string"},
]

STATIC_DATA_SCHEMA = [
    {"name": "key", "type": "string", "sort_order": "ascending"},
    {"name": "value", "type": "string", "sort_order": "ascending"},
]


HISTORY_KINDS = (
    "copy", "move", "sort", "merge", "map", "map_reduce",
    "merge_with", "alter_to_static", "alter_to_queue",
)


def _new_history():
    return {k: 0 for k in HISTORY_KINDS}


def _format_history(history):
    nonzero = {k: v for k, v in history.items() if v > 0}
    if not nonzero:
        return "{}"
    return "{" + ", ".join(f"{k}={v}" for k, v in nonzero.items()) + "}"


def _derive_history(parent, op_kind):
    new = copy.deepcopy(parent)
    new[op_kind] = new.get(op_kind, 0) + 1
    return new


def _combine_histories(a, b):
    combined = _new_history()
    for h in (a, b):
        for k, v in h.items():
            combined[k] = combined.get(k, 0) + v
    return combined


class TableBase:
    def __init__(self, base_path, name, history=None):
        self.name = name
        self.base_path = base_path
        self.path = f"{base_path}/{name}"
        self.data_path = f"{base_path}/{name}.data"
        self.history = history if history is not None else _new_history()

    def get_expected_rows(self):
        raise NotImplementedError

    def _check_rows(self, expected_rows, actual_rows, table_path, rows_descr):
        if len(actual_rows) != len(expected_rows):
            raise YtError(f"Data table {self.data_path} contains {len(expected_rows)} rows but {rows_descr} {table_path} contains {len(actual_rows)} rows")

        for expected_row, actual_row in zip(expected_rows, actual_rows):
            if expected_row["value"] != actual_row["value"]:
                raise YtError(f"Row with value '{expected_row['value']}' was expected in the {rows_descr} {table_path} but value '{actual_row['value']}' was read")

            if expected_row["key"] != actual_row["key"]:
                raise YtError(f"Row with key '{expected_row['key']}' was expected in the {rows_descr} {table_path} but key '{actual_row['key']}' was read")

    def _create_static_data_table(self, data_path, rows):
        yt.create("table", data_path, attributes={"schema": STATIC_DATA_SCHEMA})
        if rows:
            # Project to (key, value) — sources may carry extra columns
            # (e.g. Queue.get_expected_rows includes row_index).
            yt.write_table(data_path, [{"key": r["key"], "value": r["value"]} for r in rows])

    def _validate_static_result(self, result_path, result_data_path, descr):
        # Sort both sides in Python by (key, value): result_path preserves natural op
        # output order, and result_data_path may carry queue-style sort (tablet_index,
        # row_index) when the static was produced by Queue.alter_to_static (which
        # reuses the queue's .data instead of rewriting it).
        actual_rows = sorted(
            yt.read_table(result_path),
            key=lambda r: (r["key"], r["value"]),
        )
        expected_rows = sorted(
            yt.read_table(result_data_path),
            key=lambda r: (r["key"], r["value"]),
        )
        self._check_rows(expected_rows, actual_rows, result_path, descr)

    def _next_result_name(self, op_kind):
        return f"{self.name}.{op_kind}_result.{RSG.generate(8)}"

    def _input_path(self):
        # Project to (key, value) so queue inputs (which carry $cumulative_data_weight)
        # match strict (key, value) result schemas. No-op for static inputs.
        return yt.TablePath(self.path, columns=["key", "value"])

    def _create_operation_output(self, path, schema):
        # Operation outputs may later become queues and link to the shared hunk storages.
        attributes = {"schema": schema}
        cell_tag = _get_external_cell_tag(self.path)
        if cell_tag is not None:
            attributes["external_cell_tag"] = cell_tag
        yt.create("table", path, attributes=attributes)

    def _run_op_and_register(self, op_kind, output_schema, run_op_fn):
        result_name = self._next_result_name(op_kind)
        result_path = f"{self.base_path}/{result_name}"
        result_data_path = f"{self.base_path}/{result_name}.data"
        new_history = _derive_history(self.history, op_kind)

        logger.info(f"Running {op_kind} on {self.path} (result: {result_path}, new history: {_format_history(new_history)})")

        # Read expected rows OUTSIDE the master tx — Queue.get_expected_rows uses
        # select_rows (tablet tx), which does not compose with a master tx context.
        expected_rows = sorted(
            self.get_expected_rows(),
            key=lambda r: (r["key"], r["value"]),
        )

        with yt.Transaction():
            # Pre-create the result with an explicit strict schema. Operations on
            # dynamic inputs default to strict=false, which would later block
            # alter_to_queue (dynamic tables require strict=true and alter cannot
            # tighten strict).
            self._create_operation_output(result_path, output_schema)
            run_op_fn(result_path)
            self._create_static_data_table(result_data_path, expected_rows)

        self._validate_static_result(result_path, result_data_path, f"{op_kind} result")

        return StaticTable(self.base_path, result_name, history=new_history)

    def _run_sort(self):
        return self._run_op_and_register(
            "sort",
            SORTED_KV_SCHEMA,
            lambda dst: yt.run_sort(self._input_path(), dst, sort_by=["key", "value"]),
        )

    def _run_merge(self):
        mode = random.choice(["ordered", "unordered"])
        combine_chunks = random.choice([True, False])
        force_transform = random.choice([True, False])
        return self._run_op_and_register(
            "merge",
            UNSORTED_KV_SCHEMA,
            lambda dst: yt.run_merge(
                self._input_path(), dst,
                mode=mode,
                spec={"combine_chunks": combine_chunks, "force_transform": force_transform},
            ),
        )

    def _run_map_reduce(self):
        return self._run_op_and_register(
            "map_reduce",
            KEY_SORTED_KV_SCHEMA,
            lambda dst: yt.run_map_reduce(
                mapper=None, reducer=simple_reducer,
                reduce_by=["key"], sort_by=["key"],
                source_table=self._input_path(), destination_table=dst,
            ),
        )

    def _run_map(self):
        ordered = random.choice([False, True])
        return self._run_op_and_register(
            "map",
            UNSORTED_KV_SCHEMA,
            lambda dst: yt.run_map(
                simple_mapper,
                source_table=self._input_path(), destination_table=dst,
                ordered=ordered,
            ),
        )

    def run_operations(self, spec):
        results = []
        if random.random() < spec.queue_and_hunk_storage.run_sort_probability:
            results.append(self._run_sort())
        if random.random() < spec.queue_and_hunk_storage.run_merge_probability:
            results.append(self._run_merge())
        if random.random() < spec.queue_and_hunk_storage.run_map_reduce_probability:
            results.append(self._run_map_reduce())
        if random.random() < spec.queue_and_hunk_storage.run_map_probability:
            results.append(self._run_map())
        return results

    def merge_with(self, other, result_name=None):
        if result_name is None:
            result_name = f"{self.name}.merge_with.{other.name}.{RSG.generate(8)}"
        result_path = f"{self.base_path}/{result_name}"
        result_data_path = f"{self.base_path}/{result_name}.data"

        mode = random.choice(["ordered", "unordered"])
        combine_chunks = random.choice([True, False])
        force_transform = random.choice([True, False])

        new_history = _combine_histories(self.history, other.history)
        new_history["merge_with"] = new_history.get("merge_with", 0) + 1

        logger.info(
            f"Merging {self.path} and {other.path} into {result_path} "
            f"(mode: {mode}, new history: {_format_history(new_history)})"
        )

        # Project both inputs to (key, value) — queue has $cumulative_data_weight, static does not.
        input_paths = [self._input_path(), other._input_path()]

        # Read expected rows OUTSIDE the master tx — Queue.get_expected_rows uses
        # select_rows (tablet tx), which does not compose with a master tx context.
        expected_rows = sorted(
            self.get_expected_rows() + other.get_expected_rows(),
            key=lambda r: (r["key"], r["value"]),
        )

        with yt.Transaction():
            self._create_operation_output(result_path, UNSORTED_KV_SCHEMA)
            yt.run_merge(
                input_paths, result_path,
                mode=mode,
                spec={"combine_chunks": combine_chunks, "force_transform": force_transform},
            )
            self._create_static_data_table(result_data_path, expected_rows)

        self._validate_static_result(result_path, result_data_path, "merge_with result")

        return StaticTable(self.base_path, result_name, history=new_history)


class Queue(TableBase):
    def __init__(
        self, base_path, name, tablet_count, history=None, replicas_plan=None,
        cluster_name=None, replicated_table_hunks=False,
    ):
        super().__init__(base_path, name, history=history)
        self.hunk_storage_name = None
        self.mount_state = MountState(tablet_count)
        self.tablet_count = tablet_count
        self.written_row_count = [0] * tablet_count

        # replicas_plan is None for plain queues, or a list of {"mode", "hunks"} plans.
        # For replicated queues, self.path is the replicated_table receiving writes;
        # self.replicas describes the ordered tables serving reads. The source and hunk
        # replicas own their storages independently of the shared hunk_storages registry.
        self.replicas_plan = replicas_plan
        self.replicated = replicas_plan is not None
        self.replication_source = {
            "path": self.path,
            "hunks": replicated_table_hunks,
            "hunk_storage_name": None,
        } if self.replicated else None
        self.replicas = []
        self.cluster_name = cluster_name

    def create(self, attributes, erasure):
        if self.replicated:
            self._create_replicated(attributes, erasure)
        else:
            attributes = copy.deepcopy(attributes)
            attributes["dynamic"] = True
            attributes["enable_dynamic_store_read"] = True
            attributes["schema"] = QUEUE_SCHEMA
            attributes["tablet_count"] = self.tablet_count

            if erasure:
                attributes["erasure_codec"] = "isa_reed_solomon_6_3"

            logger.info(f"Creating queue {self.path}")
            yt.create("table", self.path, attributes=attributes)

        self.create_data_table()

    def _replica_path(self, index):
        return f"{self.base_path}/{self.name}.replica_{index}"

    def _replica_hunk_storage_path(self, index):
        return f"{self.base_path}/{self.name}.replica_{index}.hunk_storage"

    def _create_table_hunk_storage(self, table_path, hunk_storage_path, *, erasure):
        attributes = {"tablet_count": 1}
        if erasure:
            attributes.update(HUNK_STORAGE_ERASURE_ATTRIBUTES)
        cell_tag = _get_external_cell_tag(table_path)
        if cell_tag is not None:
            attributes["external_cell_tag"] = cell_tag

        logger.info(f"Creating hunk storage {hunk_storage_path} for table {table_path}")
        hunk_storage_id = yt.create("hunk_storage", hunk_storage_path, attributes=attributes)
        yt.mount_table(hunk_storage_path, sync=True)
        return hunk_storage_id

    def _create_replicated(self, attributes, erasure):
        logger.info(
            f"Creating replicated queue {self.path} with replicas {self.replicas_plan}, erasure: {erasure}")

        source = self.replication_source
        source["erasure"] = erasure
        replicated_table_attributes = copy.deepcopy(attributes)
        replicated_table_attributes.update({
            "dynamic": True,
            "schema": QUEUE_SCHEMA if source["hunks"] else QUEUE_SCHEMA_NO_HUNKS,
            "tablet_count": self.tablet_count,
            "in_memory_mode": "none",
        })
        # Replication logs cannot be mounted in memory, including via mount_config overrides.
        mount_config = replicated_table_attributes.setdefault("mount_config", {})
        mount_config.pop("in_memory_mode", None)
        # Async replicas must keep the shadow table's per-tablet row numbering.
        replicated_table_attributes.pop("preserve_tablet_index", None)
        mount_config["preserve_tablet_index"] = True
        if erasure:
            replicated_table_attributes["erasure_codec"] = "isa_reed_solomon_6_3"
        yt.create("replicated_table", self.path, attributes=replicated_table_attributes)
        cell_tag = _get_external_cell_tag(self.path)
        if cell_tag is not None:
            attributes = dict(attributes, external_cell_tag=cell_tag)
        if source["hunks"]:
            source["hunk_storage_name"] = f"{self.name}.hunk_storage"
            hunk_storage_id = self._create_table_hunk_storage(
                self.path, f"{self.path}.hunk_storage", erasure=erasure)
            yt.set(f"{self.path}/@hunk_storage_id", hunk_storage_id)
        yt.mount_table(self.path, sync=True)

        assert self.cluster_name is not None
        for index, plan in enumerate(self.replicas_plan):
            replica_path = self._replica_path(index)
            replica_id = yt.create("table_replica", attributes={
                "table_path": self.path,
                "cluster_name": self.cluster_name,
                "replica_path": replica_path,
                "mode": plan["mode"],
                "enable_replicated_table_tracker": False,
            })

            replica_attributes = copy.deepcopy(attributes)
            replica_attributes.update({
                "dynamic": True,
                "enable_dynamic_store_read": True,
                "upstream_replica_id": replica_id,
                # Replicas of the same source can store values inline or in hunks.
                "schema": QUEUE_SCHEMA if plan["hunks"] else QUEUE_SCHEMA_NO_HUNKS,
                "tablet_count": self.tablet_count,
            })
            if erasure:
                replica_attributes["erasure_codec"] = "isa_reed_solomon_6_3"

            yt.create("table", replica_path, attributes=replica_attributes)
            hunk_storage_name = None
            if plan["hunks"]:
                hunk_storage_path = self._replica_hunk_storage_path(index)
                hunk_storage_name = hunk_storage_path.rsplit("/", 1)[-1]
                hunk_storage_id = self._create_table_hunk_storage(
                    replica_path, hunk_storage_path, erasure=erasure)
                yt.set(f"{replica_path}/@hunk_storage_id", hunk_storage_id)

            yt.mount_table(replica_path, sync=True)
            yt.alter_table_replica(replica_id, enabled=True)

            self.replicas.append({
                "index": index,
                "path": replica_path,
                "replica_id": replica_id,
                "mode": plan["mode"],
                "hunks": plan["hunks"],
                "erasure": erasure,
                "hunk_storage_name": hunk_storage_name,
            })

        # The replicated table and all replicas were mounted synchronously above; reflect
        # that in mount_state so write()/read() treat every tablet as sync-mounted.
        self.mount_state.mount(tablet_index=None)

    def _input_path(self):
        path = self.path
        if self.replicated:
            # Operations read an ordered replica. Prefer sync replicas, whose rows are
            # visible after write(); otherwise wait for an async replica. Random selection
            # exercises both hunk and inline storage when the plan includes them.
            sync_replicas = [r for r in self.replicas if r["mode"] == "sync"]
            replica = random.choice(sync_replicas or self.replicas)
            path = replica["path"]
            if replica["mode"] == "async":
                self._wait_for_written_rows(path, list(range(self.tablet_count)))
        return yt.TablePath(path, columns=["key", "value"])

    def create_data_table(self):
        yt.create("table", self.data_path, attributes={
            "dynamic": True,
            "enable_dynamic_store_read": True,
            "schema": QUEUE_DATA_SCHEMA,
        })
        yt.mount_table(self.data_path, sync=True)

    def remove(self):
        logger.info(f"Removing queue {self.path}")
        if self.replicated:
            self._remove_replicated()
        else:
            self.unmount()
            yt.remove(self.path)
        yt.unmount_table(self.data_path, sync=True)
        yt.remove(self.data_path)

    def _remove_replicated(self):
        # Drop replicas before the source, which owns their table_replica objects.
        for table in [*self.replicas, self.replication_source]:
            yt.unmount_table(table["path"], sync=True)
            if table["hunk_storage_name"]:
                yt.remove(f"{table['path']}/@hunk_storage_id")
            yt.remove(table["path"])
            if table["hunk_storage_name"]:
                yt.remove(f"{self.base_path}/{table['hunk_storage_name']}")

    def relink_table_hunk_storage(self, table):
        old_path = f"{self.base_path}/{table['hunk_storage_name']}"
        table["hunk_storage_gen"] = table.get("hunk_storage_gen", 0) + 1
        new_path = f"{table['path']}.hunk_storage_{table['hunk_storage_gen']}"
        logger.info(f"Relinking table {table['path']} hunk storage -> {new_path}")
        new_hunk_storage_id = self._create_table_hunk_storage(
            table["path"], new_path, erasure=table["erasure"])
        yt.unmount_table(table["path"], sync=True)
        yt.set(f"{table['path']}/@hunk_storage_id", new_hunk_storage_id)
        table["hunk_storage_name"] = new_path.rsplit("/", 1)[-1]
        # The unmount flushed the table; its chunks keep references to the old hunk chunks.
        yt.remove(old_path)
        yt.mount_table(table["path"], sync=True)

    def copy(self, name):
        # Replicated queues are not copied: a yt.copy of a replica keeps its immutable
        # @upstream_replica_id (so it stays a non-writable replica), and rebuilding a plain
        # queue by replaying the shadow would exercise no copy machinery at all. Instead we
        # run map/merge/sort operations over a replica into separate outputs (see run_operations
        # / _input_path). The _copy pass skips replicated queues, so this is never reached for
        # them; assert to be safe.
        assert not self.replicated, "replicated queues are not copied"

        copy_path = f"{self.base_path}/{name}"
        new_history = _derive_history(self.history, "copy")
        logger.info(f"Copying queue {self.path} to {copy_path} (new history: {_format_history(new_history)})")

        if self.mount_state.has_mounted_tablet():
            self.unmount()
        yt.copy(self.path, copy_path)

        copied_data_path = f"{self.base_path}/{name}.data"
        yt.unmount_table(self.data_path, sync=True)
        yt.copy(self.data_path, copied_data_path)
        yt.mount_table(self.data_path, sync=True)
        yt.mount_table(copied_data_path, sync=True)

        copy_queue = Queue(self.base_path, name, self.tablet_count, history=new_history)
        copy_queue.hunk_storage_name = self.hunk_storage_name
        copy_queue.written_row_count = copy.deepcopy(self.written_row_count)

        return copy_queue

    def move(self, name):
        new_path = f"{self.base_path}/{name}"
        new_history = _derive_history(self.history, "move")
        logger.info(f"Moving queue {self.path} to {new_path} (new history: {_format_history(new_history)})")

        if self.mount_state.has_mounted_tablet():
            self.unmount()
        yt.move(self.path, new_path)

        moved_data_path = f"{self.base_path}/{name}.data"
        yt.unmount_table(self.data_path, sync=True)
        yt.move(self.data_path, moved_data_path)
        yt.mount_table(moved_data_path, sync=True)

        moved_queue = Queue(self.base_path, name, self.tablet_count, history=new_history)
        moved_queue.hunk_storage_name = self.hunk_storage_name
        moved_queue.written_row_count = copy.deepcopy(self.written_row_count)

        return moved_queue

    def mount(self, tablet_index=None, sync=True):
        logger.info(f"Mounting queue {self.path} (tablet_index: {tablet_index}, sync: {sync})")

        wait_for_pending_unmounts(self, tablet_index)

        if tablet_index is not None:
            yt.mount_table(self.path, first_tablet_index=tablet_index, last_tablet_index=tablet_index, sync=sync)
        else:
            yt.mount_table(self.path, sync=sync)

        self.mount_state.mount(tablet_index, sync=sync)

    def unmount(self, tablet_index=None, sync=True):
        logger.info(f"Unmounting queue {self.path} (tablet_index: {tablet_index}, sync: {sync})")

        wait_for_pending_mounts(self, tablet_index)

        if tablet_index is not None:
            yt.unmount_table(self.path, first_tablet_index=tablet_index, last_tablet_index=tablet_index, sync=sync)
        else:
            yt.unmount_table(self.path, sync=sync)

        self.mount_state.unmount(tablet_index, sync=sync)

    def wait_for_unmount(self, tablet_indexes):
        if not tablet_indexes:
            return

        wait_for_tablet_state(self.path, tablet_indexes, "unmounted")
        for tablet_index in tablet_indexes:
            self.mount_state.unmount(tablet_index)

    def write(self, only_in_sync_mounted, spec, retry_count):
        cfg = spec.queue_and_hunk_storage
        batch_size = random.randint(cfg.write_min_batch_size, cfg.write_max_batch_size)
        logger.info(f"Writing to the queue {self.path}, only in sync mounted: {only_in_sync_mounted}, batch size: {batch_size}")

        if only_in_sync_mounted:
            tablets = [tablet_index for tablet_index in range(self.tablet_count) if self.mount_state.is_mounted_tablet[tablet_index] and self.mount_state.is_sync[tablet_index]]
        else:
            tablets = [tablet_index for tablet_index in range(self.tablet_count) if not (not self.mount_state.is_mounted_tablet[tablet_index] and self.mount_state.is_sync[tablet_index])]

        logger.info(f"Rows will be written in tablets {tablets} in the queue {self.path}")

        if not tablets:
            logger.info(f"No mounted tablet in the queue {self.path}, do nothing")
            return

        # Precompute only the lightweight per-row placement (tablet + row index) for the
        # whole batch; the heavy key/value payloads are generated lazily below. The plan
        # is fixed up front so row indexes stay stable across insert retries even though
        # the payloads are regenerated each attempt.
        tablet_plan = [random.choice(tablets) for _ in range(batch_size)]
        running_count = {}
        row_indices = []
        for tablet_index in tablet_plan:
            row_indices.append(self.written_row_count[tablet_index] + running_count.get(tablet_index, 0))
            running_count[tablet_index] = running_count.get(tablet_index, 0) + 1

        # Insert the whole batch in a single tablet transaction, but generate and push it
        # in byte-bounded chunks: insert_rows serializes its entire input into one buffer
        # (see dynamic_table_commands.insert_rows), so splitting the batch into several
        # smaller insert_rows keeps peak memory at ~write_insert_chunk_bytes regardless of
        # batch size or row size. Retries regenerate the payloads; the transaction is
        # atomic and the row-count accounting below is applied only after it commits, so a
        # regenerated retry stays consistent.
        chunk_bytes = cfg.write_insert_chunk_bytes
        # The default rejects replicated-table writes when no sync replica exists.
        require_sync_replica = not self.replicated or any(
            replica["mode"] == "sync" for replica in self.replicas)

        def _insert_rows():
            with yt.Transaction(type="tablet"):
                i = 0
                while i < batch_size:
                    rows = []
                    data_rows = []
                    chunk_bytes_used = 0
                    while i < batch_size and (not rows or chunk_bytes_used < chunk_bytes):
                        tablet_index = tablet_plan[i]
                        key = RSG.generate(2)
                        value = RSG.generate(random.randint(cfg.write_min_row_size, cfg.write_max_row_size))
                        rows.append({"key": key, "value": value, "$tablet_index": tablet_index})
                        data_rows.append({"key": key, "value": value, "tablet_index": tablet_index, "row_index": row_indices[i]})
                        chunk_bytes_used += len(key) + len(value)
                        i += 1
                    yt.insert_rows(self.path, rows, require_sync_replica=require_sync_replica)
                    yt.insert_rows(self.data_path, data_rows)

        run_with_retries(
            _insert_rows,
            retry_count=retry_count,
            backoff=0.1,
            backoff_config={"policy": "constant_time", "constant_time": 0.1},
            except_action=lambda ex: logger.error(
                f"Exception during insert, try to retry: {ex.simplify()}"))

        for tablet_index, row_count in running_count.items():
            self.written_row_count[tablet_index] += row_count

        # Ordered-table commits can become visible on different replicas at different times.
        paths = [self.path]
        if self.replicated:
            paths = [replica["path"] for replica in self.replicas if replica["mode"] == "sync"]
        for path in paths:
            self._wait_for_written_rows(path, tablets)

    def _wait_for_written_rows(self, path, tablet_indexes):
        def check_written():
            tablet_infos = yt.get_tablet_infos(path, tablet_indexes)["tablets"]
            for offset, tablet_index in enumerate(tablet_indexes):
                if tablet_infos[offset]["total_row_count"] != self.written_row_count[tablet_index]:
                    return False
            return True

        logger.info(f"Checking written rows (written_row_count: {self.written_row_count}, path: {path})")
        wait_for_tablet_condition(
            check_written,
            error_message=(f"Table {path} has unexpected written row count "
                           f"(expected: {self.written_row_count})"),
        )

    def flush(self):
        logger.info(f"Flushing queue {self.path}")
        if self.mount_state.has_mounted_tablet():
            wait_for_pending_mounts(self, tablet_index=None)

            mounted_tablet_indexes = self.mount_state.get_mounted_tablet_indexes(
                tablet_index=None, sync=True)

            for command, state in (
                (yt.freeze_table, "frozen"),
                (yt.unfreeze_table, "mounted"),
            ):
                for tablet_index in mounted_tablet_indexes:
                    command(
                        self.path,
                        sync=False,
                        first_tablet_index=tablet_index,
                        last_tablet_index=tablet_index,
                    )
                wait_for_tablet_state(self.path, mounted_tablet_indexes, state)

    def get_expected_rows(self, tablet_index=None):
        where_expr = ""
        if tablet_index is not None:
            where_expr = f"where tablet_index = {tablet_index}"

        expected_rows = []
        while True:
            rows = list(yt.select_rows(f"select row_index, key, value from [{self.data_path}] {where_expr} order by tablet_index, row_index offset {len(expected_rows)} limit 100"))
            if len(rows) == 0:
                break
            expected_rows += rows

        return expected_rows

    def _wait_hunk_chunks_sealed(self):
        # alter_table(dynamic=False) requires every referenced hunk chunk to be sealed.
        # After unmount, sealing may still be in flight — poll until done.
        chunk_ids = yt.get(f"{self.path}/@chunk_ids")
        hunk_chunk_ids = [
            cid for cid in chunk_ids
            if yt.get(f"#{cid}/@chunk_type") != "table"
        ]
        if not hunk_chunk_ids:
            return
        wait(
            lambda: all(yt.get(f"#{cid}/@sealed") for cid in hunk_chunk_ids),
            error_message=f"Hunk chunks of {self.path} did not become sealed",
            timeout=300,
            sleep_backoff=1,
        )

    def alter_to_static(self, new_static_name):
        new_path = f"{self.base_path}/{new_static_name}"
        new_data_path = f"{self.base_path}/{new_static_name}.data"
        new_history = _derive_history(self.history, "alter_to_static")
        logger.info(f"Altering queue {self.path} into static table {new_path} (new history: {_format_history(new_history)})")

        # Read expected rows from the .data table while it is still mounted.
        # The .data table is a *sorted* dynamic table (QUEUE_DATA_SCHEMA sorts by
        # tablet_index, row_index), and YT forbids altering a sorted table from
        # dynamic to static ("Cannot switch mode from dynamic to static: table is
        # sorted"). So instead of altering the .data in place, we rebuild it below
        # as a freshly-created static table from these rows.
        expected_rows = sorted(
            self.get_expected_rows(),
            key=lambda r: (r["key"], r["value"]),
        )

        # Unmount queue and its .data so dynamic stores are flushed into chunks.
        self.unmount()
        yt.unmount_table(self.data_path, sync=True)

        # Hunk chunks may still be sealing after unmount; alter rejects unsealed.
        self._wait_hunk_chunks_sealed()

        # Direct alter dynamic→static on the queue itself. QUEUE_SCHEMA is unsorted
        # (ordered queue), so this is allowed. It preserves chunk_ids (including hunk
        # references), so subsequent ops exercise the real chunk lifecycle for hunked
        # tables.
        yt.move(self.path, new_path)
        yt.alter_table(new_path, dynamic=False)

        # The .data is sorted-dynamic and cannot be altered to static, so drop it and
        # recreate a static .data table (STATIC_DATA_SCHEMA) from the rows read above.
        # _validate_static_result sorts both sides in Python, so the differing column
        # order/sort is tolerated.
        yt.remove(self.data_path)

        static_table = StaticTable(self.base_path, new_static_name, history=new_history)
        static_table._create_static_data_table(new_data_path, expected_rows)
        static_table._validate_static_result(new_path, new_data_path, "alter_to_static result")
        return static_table

    def _read_table_and_check(self, path, spec):
        logger.info(f"Reading everything from {path}")

        cfg = spec.queue_and_hunk_storage

        for tablet_index in range(self.tablet_count):
            if tablet_index in self.mount_state.get_unmounted_tablet_indexes(tablet_index, sync=True):
                logger.info(f"Tablet {tablet_index} of queue {self.path} is unmounted with sync, skip reading it")
                continue

            written_row_count = self.written_row_count[tablet_index]

            # Stream the queue (pull_queue, ordered by row index) and the shadow .data
            # (select_rows, same row-index order) in lockstep pages instead of loading the
            # whole tablet into Python. pull_queue caps each page by data weight, so peak
            # memory stays bounded regardless of row size; the .data page is read at the
            # same offset with a matching limit.
            offset = 0
            while True:
                actual_rows = list(yt.pull_queue(
                    path, offset=offset, partition_index=tablet_index,
                    max_data_weight=cfg.read_page_max_data_weight))
                if len(actual_rows) == 0:
                    break

                expected_rows = list(yt.select_rows(
                    f"select row_index, key, value from [{self.data_path}] "
                    f"where tablet_index = {tablet_index} order by tablet_index, row_index "
                    f"offset {offset} limit {len(actual_rows)}"))

                if len(expected_rows) != len(actual_rows):
                    raise YtError(
                        f"Data table {self.data_path} contains {len(expected_rows)} rows for tablet "
                        f"{tablet_index} at offset {offset} but {path} returned {len(actual_rows)} rows")

                for expected_row, actual_row in zip(expected_rows, actual_rows):
                    if expected_row["value"] != actual_row["value"]:
                        raise YtError(
                            f"Unexpected value in {path}, tablet {tablet_index}, "
                            f"row {expected_row['row_index']}: expected {expected_row['value']!r}, "
                            f"got {actual_row['value']!r}")

                    if expected_row["key"] != actual_row["key"]:
                        raise YtError(
                            f"Unexpected key in {path}, tablet {tablet_index}, "
                            f"row {expected_row['row_index']}: expected {expected_row['key']!r}, "
                            f"got {actual_row['key']!r}")

                offset += len(actual_rows)

            if offset != written_row_count:
                raise YtError(
                    f"Read {offset} rows from {path}, tablet {tablet_index}, "
                    f"but {written_row_count} rows were written")


class StaticTable(TableBase):
    def get_expected_rows(self):
        return list(yt.read_table(self.data_path))

    def remove(self):
        logger.info(f"Removing static table {self.path}")
        yt.remove(self.path, force=True)
        yt.remove(self.data_path, force=True)

    def copy(self, new_name):
        new_path = f"{self.base_path}/{new_name}"
        new_data_path = f"{self.base_path}/{new_name}.data"
        new_history = _derive_history(self.history, "copy")
        logger.info(f"Copying static table {self.path} to {new_path} (new history: {_format_history(new_history)})")
        yt.copy(self.path, new_path)
        yt.copy(self.data_path, new_data_path)
        return StaticTable(self.base_path, new_name, history=new_history)

    def move(self, new_name):
        new_path = f"{self.base_path}/{new_name}"
        new_data_path = f"{self.base_path}/{new_name}.data"
        new_history = _derive_history(self.history, "move")
        logger.info(f"Moving static table {self.path} to {new_path} (new history: {_format_history(new_history)})")
        yt.move(self.path, new_path)
        yt.move(self.data_path, new_data_path)
        return StaticTable(self.base_path, new_name, history=new_history)

    def alter_to_queue(self, new_queue_name):
        new_path = f"{self.base_path}/{new_queue_name}"
        new_history = _derive_history(self.history, "alter_to_queue")
        logger.info(f"Altering static table {self.path} into queue {new_path} (new history: {_format_history(new_history)})")

        # Read rows from the static table itself (not its .data) so the row_index we
        # assign matches the order pull_queue will return after the alter — chunks are
        # immutable, so static read order == post-alter pull_queue order.
        rows = list(yt.read_table(self.path))
        queue_data_rows = [
            {"tablet_index": 0, "row_index": i, "key": r["key"], "value": r["value"]}
            for i, r in enumerate(rows)
        ]

        # Two cases depending on the static's lineage:
        # (a) sorted output (sort/map_reduce) → schema has sort_order columns. We must
        #     first drop sort_order (otherwise alter to dynamic infers sorted-dynamic,
        #     which can't host an ordered queue), then alter to dynamic with
        #     ALTERED_QUEUE_SCHEMA (key/value with max_inline_hunk_size).
        # (b) unsorted static → either an unsorted op output (merge/map/merge_with,
        #     schema UNSORTED_KV_SCHEMA with no hunk column) or Queue.alter_to_static
        #     (schema may carry $cumulative_data_weight and already-hunked value). A
        #     single alter dynamic=True suffices, BUT we must not silently inherit a
        #     value column without max_inline_hunk_size — otherwise a later
        #     hunk-storage link produces a queue with hunk_storage_id yet no hunks.
        #     So preserve the existing schema (keeping $cumulative_data_weight) and
        #     just ensure the value column is hunked. Adding max_inline_hunk_size is a
        #     valid alter; only *resetting* it is forbidden, so the alter_to_static
        #     lineage (already 512) is unaffected.
        schema = yt.get(f"{self.path}/@schema")
        has_sort_order = any(col.get("sort_order") for col in schema)

        yt.move(self.path, new_path)
        yt.remove(self.data_path)

        if has_sort_order:
            yt.alter_table(new_path, schema=UNSORTED_KV_SCHEMA)
            yt.alter_table(new_path, dynamic=True, schema=ALTERED_QUEUE_SCHEMA)
        else:
            hunked_schema = copy.deepcopy(schema)
            for col in hunked_schema:
                if col["name"] == "value":
                    col["max_inline_hunk_size"] = MAX_INLINE_HUNK_SIZE
            yt.alter_table(new_path, dynamic=True, schema=hunked_schema)
        yt.set(f"{new_path}/@enable_dynamic_store_read", True)

        queue = Queue(self.base_path, new_queue_name, tablet_count=1, history=new_history)
        queue.create_data_table()
        for offset in range(0, len(queue_data_rows), DATA_TABLE_WRITE_BATCH_SIZE):
            yt.insert_rows(queue.data_path, queue_data_rows[offset:offset + DATA_TABLE_WRITE_BATCH_SIZE])
        queue.written_row_count[0] = len(queue_data_rows)

        queue.mount()

        return queue


class HunkStorage:
    def __init__(self, base_path, name, queues, cell_tag=None, tablet_count=1):
        self.name = name
        self.path = f"{base_path}/{name}"
        self.queues = queues
        self.mount_state = MountState(tablet_count)
        self.cell_tag = cell_tag
        self.hunk_storage_id = None
        self.linked_queue_names = set()
        self.tablet_count = tablet_count

    def create(self, erasure=False):
        hunk_storage_attributes = {
            "tablet_count": self.tablet_count,
        }
        if self.cell_tag:
            hunk_storage_attributes["external_cell_tag"] = self.cell_tag

        if erasure:
            hunk_storage_attributes.update(HUNK_STORAGE_ERASURE_ATTRIBUTES)

        logger.info(f"Creating hunk storage {self.path} on cell tag {self.cell_tag}, erasure: {erasure}")
        self.hunk_storage_id = yt.create(
            "hunk_storage",
            self.path,
            attributes=hunk_storage_attributes)

    def mount(self, tablet_index=None, sync=True):
        logger.info(f"Mounting hunk storage {self.path} (tablet_index: {tablet_index}, sync: {sync})")

        wait_for_pending_unmounts(self, tablet_index)

        if tablet_index is not None:
            yt.mount_table(self.path, first_tablet_index=tablet_index, last_tablet_index=tablet_index, sync=sync)
        else:
            yt.mount_table(self.path, sync=sync)

        self.mount_state.mount(tablet_index, sync=sync)

    def unmount(self, tablet_index=None, sync=True):
        logger.info(f"Unmounting hunk storage {self.path} (tablet_index: {tablet_index}, sync: {sync})")

        wait_for_pending_mounts(self, tablet_index)

        tablet_indexes = range(self.tablet_count) if tablet_index is None else [tablet_index]
        tablets = yt.get(f"{self.path}/@tablets")
        tablets = [tablets[index] for index in tablet_indexes]

        # Pending queue mounts finish first; regular and hunk unmounts then overlap.
        self._async_unmount_locking_queue_tablets(tablets)

        if tablet_index is not None:
            yt.unmount_table(
                self.path,
                first_tablet_index=tablet_index,
                last_tablet_index=tablet_index,
                sync=False,
            )
        else:
            yt.unmount_table(self.path, sync=False)

        self.mount_state.unmount(tablet_index, sync=False)

        if sync:
            self.wait_for_unmount(tablet_indexes)

    def wait_for_unmount(self, tablet_indexes):
        wait_for_hunk_storage_unmounts({self: tablet_indexes})

    def _async_unmount_locking_queue_tablets(self, tablets):
        # Owners are recorded per store and may include queues that are no longer linked here.
        lock_holders = set()
        for tablet in tablets:
            if tablet["state"] == "unmounted":
                continue

            tablet_path = f"//sys/tablets/{tablet['tablet_id']}"
            try:
                stores = yt.get(f"{tablet_path}/orchid/stores")
            except YtError as err:
                state = yt.get(f"{tablet_path}/@state")
                if state == "unmounted":
                    continue
                # The orchid disappears before the master processes the unmount acknowledgement.
                if state == "unmounting" and err.is_resolve_error():
                    continue
                raise

            for store in stores.values():
                lock_holders.update(store["tablet_locks"])

        if not lock_holders:
            return

        # Resolve each owner to one queue tablet; leave the rest of the queue mounted.
        queues_by_path = {queue.path: queue for queue in self.queues.values()}
        for tablet_id in sorted(lock_holders):
            try:
                attributes = yt.get(
                    f"//sys/tablets/{tablet_id}/@",
                    attributes=["table_path", "index"],
                )
            except YtError as err:
                if not err.is_resolve_error():
                    raise
                # A normal synchronous unmount and queue removal can finish before the hunk
                # cell commits the unlock, so orchid may still list a deleted owner.
                logger.info(f"Skipping removed lock holder {tablet_id}")
                continue

            queue_path = attributes["table_path"]
            if queue_path not in queues_by_path:
                raise YtError(f"Locking tablet {tablet_id} belongs to unknown queue {queue_path}")
            queue = queues_by_path[queue_path]
            tablet_index = attributes["index"]
            if queue.mount_state.is_mounted_tablet[tablet_index]:
                queue.unmount(tablet_index=tablet_index, sync=False)

    def remove(self):
        logger.info(f"Removing hunk_storage {self.path}")
        # Removal tears down tablets on the server without waiting for hunk locks.
        yt.remove(self.path)


def link(queue, hunk_storage):
    logger.info(f"Linking hunk storage {hunk_storage.path} and {queue.path}")

    with sync_unmount_queue_temporarily(queue):
        yt.set(f"{queue.path}/@hunk_storage_id", hunk_storage.hunk_storage_id)
        queue.hunk_storage_name = hunk_storage.name
        hunk_storage.linked_queue_names.add(queue.name)


def unlink(queue, hunk_storage):
    logger.info(f"Unlinking hunk storage {hunk_storage.path} and {queue.path}")
    with sync_unmount_queue_temporarily(queue):
        yt.remove(f"{queue.path}/@hunk_storage_id")
        queue.hunk_storage_name = None
        hunk_storage.linked_queue_names.remove(queue.name)


def is_unmounted_error(err):
    err_str = str(err)
    unmounted_substrings = ["No such tablet", "has no mounted tablets", "Unknown cell 0-0-0-0", "is not known", 'while it is in "unmounted" state']
    return err.is_tablet_not_mounted() or any(s in err_str for s in unmounted_substrings)


def test_queue_and_hunk_storage(base_path, spec, attributes, args):
    logging.getLogger('Yt').setLevel(logging.DEBUG)

    yt.config["backend"] = "rpc"
    yt.config["driver_config"] = {"enable_retries": True}
    yt.config["dynamic_table_retries"]["backoff"] = {"policy": "constant_time", "constant_time": 0.1}
    yt.config["dynamic_table_retries"]["total_timeout"] = 180000
    yt.config["tablets_ready_timeout"] = 4 * 60 * 1000
    # Give transactions a generous lifetime (default is 30s): operations run under a master
    # transaction (sort/merge/map_reduce/alter) can take minutes, and a too-short timeout makes
    # the transaction expire mid-operation ("No such transaction" on commit).
    yt.config["transaction_timeout"] = 300000

    # All table replicas created by this stress-test point to the current cluster.
    # Resolve its name once per run instead of issuing a Cypress get for every new queue.
    cluster_name = yt.get("//sys/@cluster_name")

    queues = {}
    hunk_storages = {}
    tables = {}

    removed_queue_count = 0
    removed_hunk_storage_count = 0
    removed_table_count = 0

    attributes.pop("chunk_format", None)

    def _generate_queue_name():
        return f"queue_{len(queues) + removed_queue_count}"

    def _generate_hunk_storage_name():
        return f"hunk_storage_{len(hunk_storages) + removed_hunk_storage_count}"

    def _generate_table_name():
        return f"table_{len(tables) + removed_table_count}"

    def _make_replicas_plan():
        cfg = spec.queue_and_hunk_storage
        count = random.randint(cfg.replicated_min_replicas, cfg.replicated_max_replicas)
        modes = ["sync" if random.random() < cfg.replica_sync_probability else "async" for _ in range(count)]
        return [
            {"mode": modes[i], "hunks": random.random() < cfg.replica_hunks_probability}
            for i in range(count)
        ]

    def _create_queue():
        queue_name = _generate_queue_name()
        tablet_count = random.choice(range(1, 6))
        replicas_plan = None
        replicated_table_hunks = False
        if random.random() < spec.queue_and_hunk_storage.create_replicated_probability:
            replicas_plan = _make_replicas_plan()
            replicated_table_hunks = (
                random.random() < spec.queue_and_hunk_storage.replicated_table_hunks_probability)
        queue = Queue(
            base_path,
            queue_name,
            tablet_count=tablet_count,
            replicas_plan=replicas_plan,
            cluster_name=cluster_name,
            replicated_table_hunks=replicated_table_hunks,
        )
        queue.create(attributes, erasure=random.choice([True, False]))
        queues[queue_name] = queue
        return queue

    def _create_hunk_storage():
        hunk_storage_name = _generate_hunk_storage_name()
        hunk_storage = HunkStorage(
            base_path,
            hunk_storage_name,
            queues,
            cell_tag=attributes.get("external_cell_tag"),
            tablet_count=random.choice(range(1, 6)),
        )
        hunk_storage.create(erasure=random.choice([True, False]))
        hunk_storages[hunk_storage_name] = hunk_storage

    # Link plain queues immediately so they exercise hunk storage from the first iteration.
    # Reuse the first queue's cell for all subsequent queues and shared hunk storages.
    for i in range(3):
        queue = _create_queue()
        if i == 0:
            cell_tag = _get_external_cell_tag(queue.path)
            if cell_tag is not None:
                attributes["external_cell_tag"] = cell_tag
        _create_hunk_storage()

    for queue in queues.values():
        if not queue.replicated:
            link(queue, random.choice(list(hunk_storages.values())))

    def _relink():
        for queue in queues.values():
            # The replicated source and replicas use private storages, each relinked
            # independently of the queue-level shared registry.
            if queue.replicated:
                for table in [queue.replication_source, *queue.replicas]:
                    if table["hunks"] and \
                            random.random() < spec.queue_and_hunk_storage.change_hunk_storage_probability:
                        queue.relink_table_hunk_storage(table)
                continue
            if random.random() >= spec.queue_and_hunk_storage.change_hunk_storage_probability:
                continue

            hunk_storage_name = queue.hunk_storage_name
            # The decision to fully detach the queue from any hunk storage is independent
            # of the decision to change it: with unlink_hunk_storage_probability the queue
            # is left without a hunk storage, otherwise it is linked to a random one.
            new_hunk_storage = None
            if random.random() >= spec.queue_and_hunk_storage.unlink_hunk_storage_probability:
                if hunk_storages:
                    new_hunk_storage = random.choice(list(hunk_storages.values()))
            if hunk_storage_name is None and new_hunk_storage is None:
                continue

            # Keep the queue unmounted across both changes, then restore its tablet subset.
            with sync_unmount_queue_temporarily(queue):
                if hunk_storage_name:
                    unlink(queue, hunk_storages[hunk_storage_name])
                if new_hunk_storage is not None:
                    link(queue, new_hunk_storage)

    def _get_mounted_queue_tablets():
        return [
            (queue, tablet_index)
            for queue in queues.values()
            if not queue.replicated
            for tablet_index, mounted in enumerate(queue.mount_state.is_mounted_tablet)
            if mounted
        ]

    def _sync_restore_queue_tablets(mounted_tablets):
        # Restore only tablets temporarily unmounted for hunk locks.
        restored_tablets = {}
        for queue, tablet_index in mounted_tablets:
            if not queue.mount_state.is_mounted_tablet[tablet_index]:
                restored_tablets.setdefault(queue, []).append(tablet_index)

        for queue, tablet_indexes in restored_tablets.items():
            queue.wait_for_unmount(tablet_indexes)

        for queue, tablet_indexes in restored_tablets.items():
            for tablet_index in tablet_indexes:
                queue.mount(tablet_index=tablet_index, sync=False)

        for queue, tablet_indexes in restored_tablets.items():
            wait_for_tablet_state(queue.path, tablet_indexes, "mounted")
            for tablet_index in tablet_indexes:
                queue.mount_state.mount(tablet_index)

    def _remount():
        for queue in queues.values():
            if queue.replicated:
                continue
            if random.random() < spec.queue_and_hunk_storage.unmount_queue_probability:
                queue.unmount()
            else:
                queue.mount()

            for tablet_index in range(queue.tablet_count):
                if random.random() < spec.queue_and_hunk_storage.unmount_queue_tablet_probability:
                    queue.unmount(tablet_index=tablet_index, sync=False)
                elif random.random() < spec.queue_and_hunk_storage.mount_queue_tablet_probability:
                    queue.mount(tablet_index=tablet_index, sync=False)

            wait_for_pending_mounts(queue, tablet_index=None)
            wait_for_pending_unmounts(queue, tablet_index=None)

        # Remember the randomly chosen queue states before draining hunk lock holders.
        mounted_queue_tablets = _get_mounted_queue_tablets()

        hunk_tablets_to_mount = []
        for hunk_storage in hunk_storages.values():
            if random.random() < spec.queue_and_hunk_storage.unmount_hunk_storage_probability:
                hunk_storage.unmount(sync=False)
            else:
                hunk_storage.mount()

            for tablet_index in range(hunk_storage.tablet_count):
                if random.random() < spec.queue_and_hunk_storage.unmount_hunk_storage_tablet_probability:
                    hunk_storage.unmount(tablet_index=tablet_index, sync=False)
                elif random.random() < spec.queue_and_hunk_storage.mount_hunk_storage_tablet_probability:
                    hunk_tablets_to_mount.append((hunk_storage, tablet_index))

        # Finish hunk unmounts before restoring the queue tablets that released their locks.
        wait_for_hunk_storage_unmounts({
            hunk_storage: hunk_storage.mount_state.get_unmounted_tablet_indexes(None, sync=False)
            for hunk_storage in hunk_storages.values()
        })

        for hunk_storage, tablet_index in hunk_tablets_to_mount:
            hunk_storage.mount(tablet_index=tablet_index, sync=False)

        _sync_restore_queue_tablets(mounted_queue_tablets)

    def _expect_unmounted_write_error(queue, only_in_sync_mounted):
        hunk_storage = hunk_storages[queue.hunk_storage_name] if queue.hunk_storage_name else None
        return (
            (not only_in_sync_mounted and (
                queue.mount_state.has_unmounted_tablet() or
                queue.mount_state.has_mounted_tablet(sync=False))) or
            (hunk_storage is not None and
                not hunk_storage.mount_state.has_mounted_tablet(sync=True)))

    def _check_write_error(queue, only_in_sync_mounted, err):
        if _expect_unmounted_write_error(queue, only_in_sync_mounted) and is_unmounted_error(err):
            logger.info(f"Error was expected, queue or hunk_storage has unmounted tablet")
        else:
            raise err

    def _check_read_error(queue, err):
        if _has_unmount_issue(queue) and is_unmounted_error(err):
            logger.info(f"Error was expected, queue or hunk_storage has unmounted tablet")
        else:
            raise err

    def _write():
        for queue in queues.values():
            need_to_write = random.random() < spec.queue_and_hunk_storage.write_probability
            if not need_to_write:
                continue

            only_in_sync_mounted = random.choice([True, False])
            # Mount chaos deliberately creates states in which a write cannot succeed. The
            # caller below wants to observe and classify that error, not hide it behind a
            # long retry loop. A modeled healthy state still gets a few retries for genuine
            # transient tablet/cell failures.
            expected_unmounted = _expect_unmounted_write_error(queue, only_in_sync_mounted)
            retry_count = 1 if expected_unmounted else spec.queue_and_hunk_storage.write_retry_count
            try:
                queue.write(
                    only_in_sync_mounted=only_in_sync_mounted,
                    spec=spec,
                    retry_count=retry_count)
            except YtError as err:
                _check_write_error(queue, only_in_sync_mounted, err)

    def _read():
        for queue in queues.values():
            need_to_read = random.random() < spec.queue_and_hunk_storage.read_probability
            if not need_to_read:
                continue

            try:
                paths = (
                    [replica["path"] for replica in queue.replicas]
                    if queue.replicated else [queue.path]
                )
                for path in paths:
                    if queue.replicated:
                        queue._wait_for_written_rows(path, list(range(queue.tablet_count)))
                    queue._read_table_and_check(path, spec)
            except YtError as err:
                _check_read_error(queue, err)

    def _operations():
        for queue in list(queues.values()):
            try:
                for new_table in queue.run_operations(spec):
                    tables[new_table.name] = new_table
            except YtError as err:
                _check_read_error(queue, err)

    def _static_operations():
        for static_table in list(tables.values()):
            for new_table in static_table.run_operations(spec):
                tables[new_table.name] = new_table

    def _has_unmount_issue(table):
        if not isinstance(table, Queue):
            return False
        if (table.mount_state.has_unmounted_tablet() or
                table.mount_state.has_mounted_tablet(sync=False)):
            return True
        if table.hunk_storage_name and hunk_storages[table.hunk_storage_name].mount_state.has_unmounted_tablet():
            return True
        return False

    def _merge_two_tables():
        all_tables = list(queues.values()) + list(tables.values())
        if len(all_tables) < 2:
            return

        if random.random() >= spec.queue_and_hunk_storage.merge_two_tables_probability:
            return

        t1, t2 = random.sample(all_tables, 2)
        try:
            new_table = t1.merge_with(t2)
            tables[new_table.name] = new_table
        except YtError as err:
            if (_has_unmount_issue(t1) or _has_unmount_issue(t2)) and is_unmounted_error(err):
                logger.info(f"Error was expected, queue or hunk_storage has unmounted tablet")
            else:
                raise err

    def _alter_to_queue():
        nonlocal removed_table_count

        altered_names = []
        for static_table in list(tables.values()):
            if random.random() < spec.queue_and_hunk_storage.alter_to_queue_probability:
                new_queue_name = _generate_queue_name()
                queue = static_table.alter_to_queue(new_queue_name)
                queues[new_queue_name] = queue
                altered_names.append(static_table.name)

        for name in altered_names:
            del tables[name]
            removed_table_count += 1

    def _alter_to_static():
        nonlocal removed_queue_count

        altered_names = []
        for queue in list(queues.values()):
            if queue.replicated:
                continue
            if random.random() >= spec.queue_and_hunk_storage.alter_to_static_probability:
                continue

            # alter_to_static removes the queue path, which would fail while linked.
            if queue.hunk_storage_name:
                # This queue is staying unmounted for conversion, so unlink need not restore it.
                queue.unmount()
                unlink(queue, hunk_storages[queue.hunk_storage_name])

            new_static_name = _generate_table_name()
            static_table = queue.alter_to_static(new_static_name)
            tables[new_static_name] = static_table
            altered_names.append(queue.name)

        for name in altered_names:
            del queues[name]
            removed_queue_count += 1

    def _flush():
        for queue in queues.values():
            if queue.replicated:
                continue
            if random.random() < spec.queue_and_hunk_storage.flush_probability:
                queue.flush()

    def _create_and_remove():
        nonlocal removed_queue_count
        nonlocal removed_hunk_storage_count

        if random.random() < spec.queue_and_hunk_storage.create_probability:
            _create_queue()
        if random.random() < spec.queue_and_hunk_storage.create_probability:
            _create_hunk_storage()

        removed_queue_names = []
        for queue in queues.values():
            if queue.hunk_storage_name:
                continue
            if random.random() < spec.queue_and_hunk_storage.remove_probability:
                queue.remove()
                removed_queue_names += [queue.name]
                removed_queue_count += 1

        for queue_name in removed_queue_names:
            del queues[queue_name]

        removed_hunk_storage_names = []
        for hunk_storage in hunk_storages.values():
            if random.random() < spec.queue_and_hunk_storage.remove_probability:
                try:
                    hunk_storage.remove()
                    removed_hunk_storage_names += [hunk_storage.name]
                    removed_hunk_storage_count += 1
                except YtError as err:
                    if "Cannot remove a hunk storage that is being used by nodes" in str(err) and len(hunk_storage.linked_queue_names) > 0:
                        logger.info(f"Error was expected, hunk_storage has linked queues")
                    else:
                        raise err
        for hunk_storage_name in removed_hunk_storage_names:
            del hunk_storages[hunk_storage_name]

    def _copy():
        queues_to_copy = []
        for queue in queues.values():
            # Replicated queues are not copied (a copied replica stays a non-writable replica);
            # they are instead exercised by running operations over a replica (see _operations).
            if queue.replicated:
                continue
            if random.random() < spec.queue_and_hunk_storage.copy_probability:
                queues_to_copy += [queue]
        for queue in queues_to_copy:
            copy_name = _generate_queue_name()
            copy_queue = queue.copy(copy_name)
            queues[copy_name] = copy_queue

            if copy_queue.hunk_storage_name:
                hunk_storages[copy_queue.hunk_storage_name].linked_queue_names.add(copy_queue.name)

    def _move():
        nonlocal removed_queue_count

        queues_to_move = []
        for queue in queues.values():
            if queue.replicated:
                continue
            if random.random() < spec.queue_and_hunk_storage.move_probability:
                queues_to_move += [queue]
        for queue in queues_to_move:
            new_name = _generate_queue_name()
            moved_queue = queue.move(new_name)
            queues[new_name] = moved_queue

            if moved_queue.hunk_storage_name:
                hunk_storages[moved_queue.hunk_storage_name].linked_queue_names.add(moved_queue.name)
                hunk_storages[queue.hunk_storage_name].linked_queue_names.remove(queue.name)

            del queues[queue.name]
            removed_queue_count += 1

    def _copy_static_tables():
        # Snapshot before iterating so freshly-copied tables aren't copied again
        # this round; update tables in-loop so _generate_table_name stays unique.
        for table in list(tables.values()):
            if random.random() < spec.queue_and_hunk_storage.copy_static_table_probability:
                copy_name = _generate_table_name()
                new_table = table.copy(copy_name)
                tables[copy_name] = new_table

    def _move_static_tables():
        nonlocal removed_table_count
        for table in list(tables.values()):
            if random.random() < spec.queue_and_hunk_storage.move_static_table_probability:
                new_name = _generate_table_name()
                new_table = table.move(new_name)
                tables[new_name] = new_table
                del tables[table.name]
                removed_table_count += 1

    def _remove_static_tables():
        nonlocal removed_table_count
        names_to_remove = []
        for table in list(tables.values()):
            if random.random() < spec.queue_and_hunk_storage.remove_static_table_probability:
                table.remove()
                names_to_remove.append(table.name)
                removed_table_count += 1
        for name in names_to_remove:
            del tables[name]

    for iteration in range(spec.size.iterations):
        logger.iteration = iteration

        _create_and_remove()
        _relink()
        _copy()
        _move()
        _copy_static_tables()
        _move_static_tables()
        _remove_static_tables()
        _remount()

        for i in range(2):
            _write()
            _flush()
            _operations()
            _static_operations()
            _merge_two_tables()
            _alter_to_queue()
            _alter_to_static()
            _read()
