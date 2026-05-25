from .logger import logger

import yt.wrapper as yt

from yt.wrapper.retries import run_with_retries

from yt.common import YtError, wait
from lib.schema import RandomStringGenerator

import copy
import logging
import random

RSG = RandomStringGenerator()


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

    def mount(self, tablet_index, sync):
        self._mount_impl(True, tablet_index, sync)

    def unmount(self, tablet_index, sync):
        self._mount_impl(False, tablet_index, sync)

    def _mount_impl(self, is_mount, tablet_index, sync):
        if not tablet_index is not None:
            self.is_mounted_tablet = [is_mount] * self.tablet_count
            self.is_sync = [sync] * self.tablet_count
        else:
            self.is_mounted_tablet[tablet_index] = is_mount
            self.is_sync[tablet_index] = sync


def mount_async_tablets(obj, tablet_index):
    mounted_async_tablet_indexes = obj.mount_state.get_mounted_tablet_indexes(tablet_index, sync=False)
    if mounted_async_tablet_indexes:
        logger.info(f"Object {obj.path} was mounted async for tablets {mounted_async_tablet_indexes}, mounting with sync)")
        for mounted_async_tablet_index in mounted_async_tablet_indexes:
            obj.mount(mounted_async_tablet_index, sync=True)


def unmount_async_tablets(obj, tablet_index):
    unmounted_async_tablet_indexes = obj.mount_state.get_unmounted_tablet_indexes(tablet_index, sync=False)
    if unmounted_async_tablet_indexes:
        logger.info(f"Object {obj.path} was unmounted async for tablets {unmounted_async_tablet_indexes}, unmounting with sync)")
        for unmounted_async_tablet_index in unmounted_async_tablet_indexes:
            obj.unmount(unmounted_async_tablet_index, sync=True)


QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "value", "type": "string", "max_inline_hunk_size": 512},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

# Schema for queues created via alter_to_queue: no $cumulative_data_weight,
# so altering an existing static table (which has key/value only) is a pure schema
# relaxation. Static results are pre-created with strict=true so this alter is valid
# (dynamic tables require strict=true, and alter cannot tighten strict).
ALTERED_QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "value", "type": "string", "max_inline_hunk_size": 512},
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
            yt.create("table", result_path, attributes={"schema": output_schema})
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
            yt.create("table", result_path, attributes={"schema": UNSORTED_KV_SCHEMA})
            yt.run_merge(
                input_paths, result_path,
                mode=mode,
                spec={"combine_chunks": combine_chunks, "force_transform": force_transform},
            )
            self._create_static_data_table(result_data_path, expected_rows)

        self._validate_static_result(result_path, result_data_path, "merge_with result")

        return StaticTable(self.base_path, result_name, history=new_history)


class Queue(TableBase):
    def __init__(self, base_path, name, tablet_count, history=None):
        super().__init__(base_path, name, history=history)
        self.hunk_storage_name = None
        self.mount_state = MountState(tablet_count)
        self.cell_tag = None
        self.tablet_count = tablet_count
        self.written_row_count = [0] * tablet_count

    def create(self, attributes, erasure):
        attributes = copy.deepcopy(attributes)
        attributes["dynamic"] = True
        attributes["enable_dynamic_store_read"] = True
        attributes["schema"] = QUEUE_SCHEMA
        attributes["tablet_count"] = self.tablet_count

        if erasure:
            attributes["erasure_codec"] = "isa_reed_solomon_6_3"

        logger.info(f"Creating queue {self.path}")
        yt.create("table", self.path, attributes=attributes)

        if yt.exists(f"{self.path}/@external_cell_tag"):
            self.cell_tag = yt.get(f"{self.path}/@external_cell_tag")

        self.create_data_table()

    def create_data_table(self):
        yt.create("table", self.data_path, attributes={
            "dynamic": True,
            "enable_dynamic_store_read": True,
            "schema": QUEUE_DATA_SCHEMA,
        })
        yt.mount_table(self.data_path, sync=True)

    def remove(self):
        logger.info(f"Removing queue {self.path}")
        self.unmount()
        yt.remove(self.path)
        yt.unmount_table(self.data_path, sync=True)
        yt.remove(self.data_path)

    def copy(self, name):
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

        unmount_async_tablets(self, tablet_index)

        if tablet_index is not None:
            yt.mount_table(self.path, first_tablet_index=tablet_index, last_tablet_index=tablet_index, sync=sync)
        else:
            yt.mount_table(self.path, sync=sync)

        self.mount_state.mount(tablet_index, sync)

    def unmount(self, tablet_index=None, sync=True):
        logger.info(f"Unmounting queue {self.path} (tablet_index: {tablet_index}, sync: {sync})")

        mount_async_tablets(self, tablet_index)

        if tablet_index is not None:
            yt.unmount_table(self.path, first_tablet_index=tablet_index, last_tablet_index=tablet_index, sync=sync)
        else:
            yt.unmount_table(self.path, sync=sync)

        self.mount_state.unmount(tablet_index, sync)

    def write(self, only_in_sync_mounted):
        logger.info(f"Writing to the queue {self.path}, only in sync mounted: {only_in_sync_mounted}")

        if only_in_sync_mounted:
            tablets = [tablet_index for tablet_index in range(self.tablet_count) if self.mount_state.is_mounted_tablet[tablet_index] and self.mount_state.is_sync[tablet_index]]
        else:
            tablets = [tablet_index for tablet_index in range(self.tablet_count) if not (not self.mount_state.is_mounted_tablet[tablet_index] and self.mount_state.is_sync[tablet_index])]

        logger.info(f"Rows will be written in tablets {tablets} in the queue {self.path}")

        if not tablets:
            logger.info(f"No mounted tablet in the queue {self.path}, do nothing")
            return

        rows = [{"key": RSG.generate(2), "value": RSG.generate(1024), "$tablet_index": random.choice(tablets)} for _ in range(10)]
        data_rows = []

        new_written_row_count = [0] * self.tablet_count
        for row in rows:
            tablet_index = row["$tablet_index"]
            row_index = self.written_row_count[tablet_index] + new_written_row_count[tablet_index]
            data_rows += [{"key": row["key"], "value": row["value"], "tablet_index": tablet_index, "row_index": row_index}]
            new_written_row_count[row["$tablet_index"]] += 1

        logger.info(f"Rows to write in the queue {self.path}: {rows}")

        def _insert_rows():
            with yt.Transaction(type="tablet"):
                yt.insert_rows(self.path, rows)
                yt.insert_rows(self.data_path, data_rows)

        run_with_retries(lambda: _insert_rows(), retry_count=1800, backoff=0.1, backoff_config={"policy": "constant_time", "constant_time": 0.1}, except_action=lambda ex: logger.error(f"Exception during insert, try to retry: {ex.simplify()}"))

        for tablet_index, row_count in enumerate(new_written_row_count):
            self.written_row_count[tablet_index] += row_count

        def check_written():
            tablet_infos = yt.get_tablet_infos(self.path, tablets)["tablets"]
            for offset, tablet_index in enumerate(tablets):
                if tablet_infos[offset]["total_row_count"] != self.written_row_count[tablet_index]:
                    return False
            return True

        logger.info(f"Checking written rows (written_row_count: {self.written_row_count}, queue: {self.path})")

        wait(check_written, error_message=f"Queue {self.path} has unexpected written row count (expected: {self.written_row_count})")

    def flush(self):
        logger.info(f"Flushing queue {self.path}")
        if self.mount_state.has_mounted_tablet():
            mount_async_tablets(self, tablet_index=None)

            mounted_tablet_indexes = self.mount_state.get_mounted_tablet_indexes(tablet_index=None, sync=True)
            for tablet_index in mounted_tablet_indexes:
                yt.freeze_table(self.path, sync=True, first_tablet_index=tablet_index, last_tablet_index=tablet_index)
                yt.unfreeze_table(self.path, sync=True, first_tablet_index=tablet_index, last_tablet_index=tablet_index)

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

        # Unmount queue and its .data so dynamic stores are flushed into chunks.
        self.unmount()
        yt.unmount_table(self.data_path, sync=True)

        # Hunk chunks may still be sealing after unmount; alter rejects unsealed.
        self._wait_hunk_chunks_sealed()

        # Direct alter dynamic→static on both the queue and its .data. Preserves
        # chunk_ids (including hunk references on the queue), so subsequent ops
        # exercise the real chunk lifecycle for hunked tables. The .data carries
        # queue-style sort (tablet_index, row_index) into the static result —
        # _validate_static_result sorts both sides in Python to tolerate that.
        yt.move(self.path, new_path)
        yt.alter_table(new_path, dynamic=False)
        yt.move(self.data_path, new_data_path)
        yt.alter_table(new_data_path, dynamic=False)

        static_table = StaticTable(self.base_path, new_static_name, history=new_history)
        static_table._validate_static_result(new_path, new_data_path, "alter_to_static result")
        return static_table

    def read_and_check(self):
        logger.info(f"Reading everything from queue {self.path}")

        for tablet_index in range(self.tablet_count):
            if tablet_index in self.mount_state.get_unmounted_tablet_indexes(tablet_index, sync=True):
                logger.info(f"Tablet {tablet_index} of queue {self.path} is unmounted with sync, skip reading it")
                continue
            actual_rows = []
            while True:
                rows = list(yt.pull_queue(self.path, offset=len(actual_rows), partition_index=tablet_index))
                if len(rows) == 0:
                    break
                actual_rows += rows
            written_row_count = self.written_row_count[tablet_index]
            if len(actual_rows) != written_row_count:
                raise YtError(f"From queue {self.path} from tablet {tablet_index} were read {len(actual_rows)} rows but {written_row_count} rows were written")

            expected_rows = self.get_expected_rows(tablet_index)

            if len(actual_rows) != len(expected_rows):
                raise YtError(f"Data table {self.data_path} contains {len(expected_rows)} rows for tablet {tablet_index} but queue {self.path} contains {len(actual_rows)} rows")

            for expected_row, actual_row in zip(expected_rows, actual_rows):
                if expected_row["value"] != actual_row["value"]:
                    raise YtError(f"Row with value '{expected_row['value']}' was expected in the queue {self.path} in the tablet {tablet_index} but value '{actual_row['value']}' was read")

                if expected_row["key"] != actual_row["key"]:
                    raise YtError(f"Row with key '{expected_row['key']}' was expected in the queue {self.path} in the tablet {tablet_index} but key '{actual_row['key']}' was read")


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
        # (a) came from sort/map_reduce/merge → schema has sort_order columns. We must
        #     first drop sort_order (otherwise alter to dynamic infers sorted-dynamic,
        #     which can't host an ordered queue), then alter to dynamic with
        #     ALTERED_QUEUE_SCHEMA (key/value, no $cumulative_data_weight).
        # (b) came from Queue.alter_to_static → schema lacks sort_order and may carry
        #     $cumulative_data_weight. A single alter dynamic=True suffices; preserving
        #     the existing schema (including $cumulative_data_weight) is fine for queues.
        schema = yt.get(f"{self.path}/@schema")
        has_sort_order = any(col.get("sort_order") for col in schema)

        yt.move(self.path, new_path)
        yt.remove(self.data_path)

        if has_sort_order:
            yt.alter_table(new_path, schema=UNSORTED_KV_SCHEMA)
            yt.alter_table(new_path, dynamic=True, schema=ALTERED_QUEUE_SCHEMA)
        else:
            yt.alter_table(new_path, dynamic=True)
        yt.set(f"{new_path}/@enable_dynamic_store_read", True)

        queue = Queue(self.base_path, new_queue_name, tablet_count=1, history=new_history)
        queue.create_data_table()
        if queue_data_rows:
            yt.insert_rows(queue.data_path, queue_data_rows)
        queue.written_row_count[0] = len(queue_data_rows)

        queue.mount()

        return queue


class HunkStorage:
    def __init__(self, base_path, name, cell_tag=None, tablet_count=1):
        self.name = name
        self.path = f"{base_path}/{name}"
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
            hunk_storage_attributes["erasure_codec"] = "reed_solomon_3_3"
            hunk_storage_attributes["replication_factor"] = 1
            hunk_storage_attributes["read_quorum"] = 4
            hunk_storage_attributes["write_quorum"] = 5

        logger.info(f"Creating hunk storage {self.path} on cell tag {self.cell_tag}, erasure: {erasure}")
        self.hunk_storage_id = yt.create(
            "hunk_storage",
            self.path,
            attributes=hunk_storage_attributes)

    def mount(self, tablet_index=None, sync=True):
        logger.info(f"Mounting hunk storage {self.path} (tablet_index: {tablet_index}, sync: {sync})")

        unmount_async_tablets(self, tablet_index)

        if tablet_index is not None:
            yt.mount_table(self.path, first_tablet_index=tablet_index, last_tablet_index=tablet_index, sync=sync)
        else:
            yt.mount_table(self.path, sync=sync)

        self.mount_state.mount(tablet_index, sync)

    def unmount(self, tablet_index=None, sync=True):
        logger.info(f"Unmounting hunk storage {self.path} (tablet_index: {tablet_index}, sync: {sync})")

        mount_async_tablets(self, tablet_index)

        if tablet_index is not None:
            yt.unmount_table(self.path, first_tablet_index=tablet_index, last_tablet_index=tablet_index, sync=sync)
        else:
            yt.unmount_table(self.path, sync=sync)

        self.mount_state.unmount(tablet_index, sync)

    def remove(self):
        logger.info(f"Removing hunk_storage {self.path}")
        self.unmount()
        yt.remove(self.path)


def link(queue, hunk_storage):
    logger.info(f"Linking hunk storage {hunk_storage.path} and {queue.path}")
    # TODO: check cell tags.

    yt.set(f"{queue.path}/@hunk_storage_id", hunk_storage.hunk_storage_id)
    yt.remount_table(queue.path)
    queue.hunk_storage_name = hunk_storage.name
    hunk_storage.linked_queue_names.add(queue.name)


def unlink(queue, hunk_storage):
    logger.info(f"Unlinking hunk storage {hunk_storage.path} and {queue.path}")
    yt.remove(f"{queue.path}/@hunk_storage_id")
    yt.remount_table(queue.path)
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

    # Enable dynamic→static alter for queues with hunks; required by
    # Queue.alter_to_static which uses alter_table(dynamic=False) directly.
    yt.set("//sys/@config/tablet_manager/enable_alter_to_static_with_hunks", True)

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

    def _create_queue():
        queue_name = _generate_queue_name()
        queue = Queue(base_path, queue_name, tablet_count=random.choice(range(1, 6)))
        queue.create(attributes, erasure=random.choice([True, False]))
        queues[queue_name] = queue

    def _create_hunk_storage():
        hunk_storage_name = _generate_hunk_storage_name()
        hunk_storage = HunkStorage(base_path, hunk_storage_name, tablet_count=random.choice(range(1, 6)))
        hunk_storage.create(erasure=random.choice([True, False]))
        hunk_storages[hunk_storage_name] = hunk_storage

    # Creating initial queues and hunk storages.
    for i in range(3):
        _create_queue()
        _create_hunk_storage()

    def _relink():
        for queue in queues.values():
            if random.random() < spec.queue_and_hunk_storage.change_hunk_storage_probability:
                hunk_storage_name = queue.hunk_storage_name
                if hunk_storage_name:
                    unlink(queue, hunk_storages[hunk_storage_name])

                new_hunk_storage_name = random.choice(list(hunk_storages) + [None])
                if new_hunk_storage_name:
                    link(queue, hunk_storages[new_hunk_storage_name])

    def _remount():
        for queue in queues.values():
            if random.random() < spec.queue_and_hunk_storage.unmount_queue_probability:
                queue.unmount()
            else:
                queue.mount()

            for tablet_index in range(queue.tablet_count):
                if random.random() < spec.queue_and_hunk_storage.unmount_queue_tablet_probability:
                    queue.unmount(tablet_index=tablet_index)
                elif random.random() < spec.queue_and_hunk_storage.mount_queue_tablet_probability:
                    queue.mount(tablet_index=tablet_index)

        for hunk_storage in hunk_storages.values():
            if random.random() < spec.queue_and_hunk_storage.unmount_hunk_storage_probability:
                if len(hunk_storage.linked_queue_names) > 0:
                    for queue_name in hunk_storage.linked_queue_names:
                        queues[queue_name].unmount()
                hunk_storage.unmount()
            else:
                hunk_storage.mount()

            for tablet_index in range(hunk_storage.tablet_count):
                if random.random() < spec.queue_and_hunk_storage.unmount_hunk_storage_tablet_probability:
                    hunk_storage.unmount(tablet_index=tablet_index, sync=False)
                elif random.random() < spec.queue_and_hunk_storage.mount_hunk_storage_tablet_probability:
                    hunk_storage.mount(tablet_index=tablet_index, sync=False)

    def _check_write_error(queue, only_in_sync_mounted, err):
        unmounted = (not only_in_sync_mounted and queue.mount_state.has_unmounted_tablet()) or (queue.hunk_storage_name and not hunk_storages[queue.hunk_storage_name].mount_state.has_mounted_tablet(sync=True))
        if unmounted and is_unmounted_error(err):
            logger.info(f"Error was expected, queue or hunk_storage has unmounted tablet")
        else:
            raise err

    def _check_read_error(queue, err):
        unmounted = queue.mount_state.has_unmounted_tablet() or (queue.hunk_storage_name and hunk_storages[queue.hunk_storage_name].mount_state.has_unmounted_tablet())
        if unmounted and is_unmounted_error(err):
            logger.info(f"Error was expected, queue or hunk_storage has unmounted tablet")
        else:
            raise err

    def _write():
        for queue in queues.values():
            need_to_write = random.random() < spec.queue_and_hunk_storage.write_probability
            if not need_to_write:
                continue

            only_in_sync_mounted = random.choice([True, False])
            try:
                queue.write(only_in_sync_mounted=only_in_sync_mounted)
            except YtError as err:
                _check_write_error(queue, only_in_sync_mounted, err)

    def _read():
        for queue in queues.values():
            need_to_read = random.random() < spec.queue_and_hunk_storage.read_probability
            if not need_to_read:
                continue

            try:
                queue.read_and_check()
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
        if table.mount_state.has_unmounted_tablet():
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
            if random.random() >= spec.queue_and_hunk_storage.alter_to_static_probability:
                continue

            # alter_to_static removes the queue path, which would fail while linked.
            if queue.hunk_storage_name:
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
            if random.random() < spec.queue_and_hunk_storage.remove_probability:
                try:
                    queue.remove()
                    removed_queue_names += [queue.name]
                    removed_queue_count += 1

                    if queue.hunk_storage_name:
                        hunk_storages[queue.hunk_storage_name].linked_queue_names.remove(queue.name)
                except YtError as err:
                    if "Cannot remove table" in str(err) and "that is linked to hunk storage" in str(err) and queue.hunk_storage_name:
                        logger.info(f"Error was expected, queue is linked to hunk storage")
                    else:
                        raise err

        for queue_name in removed_queue_names:
            del queues[queue_name]

        removed_hunk_storage_names = []
        for hunk_storage in hunk_storages.values():
            if random.random() < spec.queue_and_hunk_storage.remove_probability:
                try:
                    if len(hunk_storage.linked_queue_names) > 0:
                        logger.info(f"Removing hunk storage {hunk_storage.name}, need to unmount linked queues {hunk_storage.linked_queue_names}")
                        for queue_name in hunk_storage.linked_queue_names:
                            queues[queue_name].unmount()

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
