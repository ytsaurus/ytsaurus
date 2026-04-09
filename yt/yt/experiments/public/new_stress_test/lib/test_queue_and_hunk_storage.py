from .logger import logger

import yt.wrapper as yt

from yt.wrapper.retries import run_with_retries

from yt.common import YtError, wait
from lib.schema import RandomStringGenerator

import copy
import logging
import random

RSG = RandomStringGenerator()

@yt.with_context
def simple_mapper(input_row, context):
    output_row = {
        "tablet_index": context.tablet_index,
        "row_index": context.row_index,
        "key": input_row["key"],
        "value": input_row["value"],
    }
    yield output_row


@yt.with_context
def simple_reducer(key, input_row_iterator, context):
    for input_row in input_row_iterator:
        output_row = {
            "key": key["key"],
            "value": input_row["value"],
        }
        yield output_row


class MountState:
    def __init__(self, tablet_count):
        self.tablet_count = tablet_count
        self.is_mounted_tablet = [False] * tablet_count
        self.is_sync = [False] * tablet_count

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
        tablets = [tablet_index] if tablet_index else [tablet_index for tablet_index in range(self.tablet_count)]
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
        logger.info(f"Object {obj.path} was unmounted async for tablets {unmounted_async_tablet_indexes}, mounting with sync)")
        for unmounted_async_tablet_index in unmounted_async_tablet_indexes:
            obj.unmount(unmounted_async_tablet_index, sync=True)


class Queue:
    def __init__(self, base_path, name, tablet_count):
        self.name = name
        self.base_path = base_path
        self.path = f"{self.base_path}/{name}"
        self.data_path = f"{self.base_path}/{name}.data"
        self.hunk_storage_name = None
        self.mount_state = MountState(tablet_count)
        self.data_path = self.path + ".data"
        self.cell_tag = None
        self.tablet_count = tablet_count
        self.written_row_count = [0] * tablet_count

    def create(self, attributes, erasure):
        attributes = copy.deepcopy(attributes)
        attributes["dynamic"] = True
        attributes["enable_dynamic_store_read"] = True
        attributes["schema"] = [
            {"name": "key", "type": "string"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 512},
            {"name": "$cumulative_data_weight", "type": "int64"}
        ]
        attributes["tablet_count"] = self.tablet_count

        if erasure:
            attributes["erasure_codec"] = "isa_reed_solomon_6_3"

        logger.info(f"Creating queue {self.path}")
        yt.create("table", self.path, attributes=attributes)

        if yt.exists(f"{self.path}/@external_cell_tag"):
            self.cell_tag = yt.get(f"{self.path}/@external_cell_tag")

        self.create_data_table()

    def create_data_table(self):
        attributes = {
            "dynamic": True,
            "enable_dynamic_store_read": True,
            "schema": [
                {"name": "tablet_index", "type": "int64", "sort_order": "ascending"},
                {"name": "row_index", "type": "int64", "sort_order": "ascending"},
                {"name": "key", "type": "string"},
                {"name": "value", "type": "string"},
            ]
        }
        yt.create("table", self.data_path, attributes=attributes)
        yt.mount_table(self.data_path, sync=True)

    def remove(self):
        logger.info(f"Removing queue {self.path}")
        self.unmount()
        yt.remove(self.path)

    def copy(self, name):
        copy_path = f"{self.base_path}/{name}"
        logger.info(f"Copying queue {self.path} to {copy_path}")

        if self.mount_state.has_mounted_tablet():
            self.unmount()
        yt.copy(self.path, copy_path)

        copied_data_path = f"{self.base_path}/{name}.data"
        yt.unmount_table(self.data_path, sync=True)
        yt.copy(self.data_path, copied_data_path)
        yt.mount_table(self.data_path, sync=True)
        yt.mount_table(copied_data_path, sync=True)

        copy_queue = Queue(self.base_path, name, self.tablet_count)
        copy_queue.hunk_storage_name = self.hunk_storage_name
        copy_queue.data_path = copied_data_path

        return copy_queue

    def move(self, name):
        new_path = f"{self.base_path}/{name}"
        logger.info(f"Moving queue {self.path} to {new_path}")

        if self.mount_state.has_mounted_tablet():
            self.unmount()
        yt.move(self.path, new_path)

        moved_data_path = f"{self.base_path}/{name}.data"
        yt.unmount_table(self.data_path, sync=True)
        yt.move(self.data_path, moved_data_path)
        yt.mount_table(moved_data_path, sync=True)

        moved_queue = Queue(self.base_path, name, self.tablet_count)
        moved_queue.hunk_storage_name = self.hunk_storage_name
        moved_queue.data_path = moved_data_path

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

    def write(self, only_in_mounted):
        logger.info(f"Writing to the queue {self.path}, only in mounted: {only_in_mounted}")

        if only_in_mounted:
            tablets = [tablet_index for tablet_index in range(self.tablet_count) if self.mount_state.is_mounted_tablet[tablet_index] and self.mount_state.is_sync[tablet_index]]
        else:
            tablets = [tablet_index for tablet_index in range(self.tablet_count)]

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

        def _insert_rows():
            with yt.Transaction(type="tablet"):
                yt.insert_rows(self.path, rows)
                yt.insert_rows(self.data_path, data_rows)

        run_with_retries(lambda: _insert_rows(), retry_count=5, backoff=2, except_action=lambda ex: logger.error(f"Exception during insert, try to retry: {ex.simplify()}"))

        for tablet_index, row_count in enumerate(new_written_row_count):
            self.written_row_count[tablet_index] += row_count

        def check_written():
            tablet_infos = yt.get_tablet_infos(self.path, tablets)["tablets"]
            for offset, tablet_index in enumerate(tablets):
                if tablet_infos[offset]["total_row_count"] != self.written_row_count[tablet_index]:
                    return False
            return True

        logger.info(f"Checking written rows (written_row_count: {self.written_row_count})")

        wait(check_written)

    def flush(self):
        logger.info(f"Flushing queue {self.path}")
        if self.mount_state.has_mounted_tablet():
            mount_async_tablets(self, tablet_index=None)

            mounted_tablet_indexes = self.mount_state.get_mounted_tablet_indexes(tablet_index=None, sync=True)
            for tablet_index in mounted_tablet_indexes:
                yt.freeze_table(self.path, sync=True, first_tablet_index=tablet_index, last_tablet_index=tablet_index)
                yt.unfreeze_table(self.path, sync=True, first_tablet_index=tablet_index, last_tablet_index=tablet_index)

    def _get_expected_rows(self, tablet_index=None):
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

    def read_and_check(self):
        logger.info(f"Reading everything from queue {self.path}")

        for tablet_index in range(self.tablet_count):
            actual_rows = []
            while True:
                rows = list(yt.pull_queue(self.path, offset=len(actual_rows), partition_index=tablet_index))
                if len(rows) == 0:
                    break
                actual_rows += rows
            written_row_count = self.written_row_count[tablet_index]
            if len(actual_rows) != written_row_count:
                raise YtError(f"From queue {self.path} from tablet {tablet_index} were read {len(actual_rows)} rows but {written_row_count} rows were written")

            expected_rows = self._get_expected_rows(tablet_index)

            if len(actual_rows) != len(expected_rows):
                raise YtError(f"Data table {self.data_path} contains {len(expected_rows)} rows for tablet {tablet_index} but queue {self.path} contains {len(actual_rows)} rows")

            for expected_row, actual_row in zip(expected_rows, actual_rows):
                if expected_row["value"] != actual_row["value"]:
                    raise YtError(f"Row with value '{expected_row["value"]}' was expected in the queue {self.path} in the tablet {tablet_index} but value '{actual_row["value"]}' was read")

                if expected_row["key"] != actual_row["key"]:
                    raise YtError(f"Row with key '{expected_row["key"]}' was expected in the queue {self.path} in the tablet {tablet_index} but key '{actual_row["key"]}' was read")

    def _check_rows(self, expected_rows, actual_rows, table_path, rows_descr,):

        if len(actual_rows) != len(expected_rows):
            raise YtError(f"Data table {self.data_path} contains {len(expected_rows)} rows but {rows_descr} {table_path} contains {len(actual_rows)} rows")

        for expected_row, actual_row in zip(expected_rows, actual_rows):
            if expected_row["value"] != actual_row["value"]:
                raise YtError(f"Row with value '{expected_row["value"]}' was expected in the {rows_descr} {table_path} but value '{actual_row["value"]}' was read")

            if expected_row["key"] != actual_row["key"]:
                raise YtError(f"Row with key '{expected_row["key"]}' was expected in the {rows_descr} {table_path} but key '{actual_row["key"]}' was read")

    def run_map(self):
        logger.info(f"Running map on queue {self.path}")
        map_result_path = self.path + ".map_result"

        yt.run_map(simple_mapper, source_table=self.path, destination_table=map_result_path, spec={"job_io": {"control_attributes": {"enable_row_index": True, "enable_tablet_index": True}}})

        yt.run_sort(map_result_path, sort_by=["tablet_index", "row_index"])
        map_result_rows = list(yt.read_table(map_result_path))

        expected_rows = self._get_expected_rows()
        self._check_rows(expected_rows, map_result_rows, map_result_path, "map result")


    def run_sort(self):
        logger.info(f"Running sort on queue {self.path}")
        sort_result_path = self.path + ".sort_result"

        yt.run_sort(self.path, sort_result_path, sort_by=["key", "value"])

        sort_result_rows = list(yt.read_table(sort_result_path))

        expected_rows = self._get_expected_rows()
        expected_rows = sorted(expected_rows, key=lambda x: (x["key"], x["value"]))

        self._check_rows(expected_rows, sort_result_rows, sort_result_path, "sort result")


    def run_map_reduce(self):
        logger.info(f"Running map-reduce on queue {self.path}")
        map_reduce_result_path = self.path + ".map_reduce_result"

        yt.run_map_reduce(mapper=None, reducer=simple_reducer, reduce_by=["key"], sort_by=["key"], source_table=self.path, destination_table=map_reduce_result_path)

        yt.run_sort(map_reduce_result_path, sort_by=["key", "value"])

        map_reduce_result_rows = list(yt.read_table(map_reduce_result_path))

        expected_rows = self._get_expected_rows()
        expected_rows = sorted(expected_rows, key=lambda x: (x["key"], x["value"]))

        self._check_rows(expected_rows, map_reduce_result_rows, map_reduce_result_path, "map-reduce result")


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

    queues = {}
    hunk_storages = {}
    removed_queue_count = 0
    removed_hunk_storage_count = 0

    attributes.pop("chunk_format", None)

    def _generate_queue_name():
        return f"queue_{len(queues) + removed_queue_count}"

    def _generate_hunk_storage_name():
        return f"hunk_storage_{len(hunk_storages) + removed_hunk_storage_count}"

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


    def _check_write_error(queue, err):
        unmounted = queue.mount_state.has_unmounted_tablet() or (queue.hunk_storage_name and not hunk_storages[queue.hunk_storage_name].mount_state.has_mounted_tablet(sync=True))
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
            queue.write(only_in_mounted=True)

            try:
                queue.write(only_in_mounted=False)
            except YtError as err:
                _check_write_error(queue, err)


    def _read():
        for queue in queues.values():
            try:
                queue.read_and_check()
            except YtError as err:
                _check_read_error(queue, err)


    def _map():
        for queue in queues.values():
            try:
                queue.run_map()
            except YtError as err:
                _check_read_error(queue, err)

    def _map_reduce():
        for queue in queues.values():
            try:
                queue.run_map_reduce()
            except YtError as err:
                _check_read_error(queue, err)


    def _sort():
        for queue in queues.values():
            try:
                queue.run_sort()
            except YtError as err:
                _check_read_error(queue, err)


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
        new_queues = []
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

    for iteration in range(spec.size.iterations):
        logger.iteration = iteration

        _create_and_remove()
        _relink()
        _copy()
        _move()
        _remount()

        for i in range(10):
            _write()
            _flush()

            _map()
            _map_reduce()
            _sort()

            _read()

        # TODO(nadya73): add alter queue the the static tables and running maps in 25.4.
