from .logger import logger

import yt.wrapper as yt

from yt.common import YtError, wait
from lib.schema import RandomStringGenerator

import copy
import logging
import random

RSG = RandomStringGenerator()

class Queue:
    def __init__(self, base_path, name, tablet_count):
        self.name = name
        self.path = f"{base_path}/{name}"
        self.data_path = f"{base_path}/{name}.data"
        self.hunk_storage_name = None
        self.mounted = False
        self.hunk_storage_mounted = False
        self.data_path = self.path + ".data"
        self.cell_tag = None
        self.tablet_count = tablet_count
        self.written_row_count = [0] * tablet_count

    def create(self, attributes, erasure):
        attributes = copy.deepcopy(attributes)
        attributes["dynamic"] = True
        attributes["enable_dynamic_store_read"] = True
        attributes["schema"] = [
            {"name": "value", "type": "string", "max_inline_hunk_size": 512},
            {"name": "$cumulative_data_weight", "type": "int64"}
        ]
        attributes["tablet_count"] = self.tablet_count

        if erasure:
            attributes["erasure_codec"] = "isa_reed_solomon_6_3"

        logger.info(f"Creating queue {self.path}")
        yt.create("table", self.path, attributes=attributes)


        attributes["schema"] = [
            {"name": "tablet_index", "type": "int64", "sort_order": "ascending"},
            {"name": "row_index", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]
        yt.create("table", self.data_path, attributes=attributes)
        yt.mount_table(self.data_path, sync=True)

        if yt.exists(f"{self.path}/@external_cell_tag"):
            self.cell_tag = yt.get(f"{self.path}/@external_cell_tag")

    def remove(self):
        logger.info(f"Removing queue {self.path}")
        self.unmount()
        yt.remove(self.path)

    def mount(self):
        logger.info(f"Mounting queue {self.path}")
        yt.mount_table(self.path, sync=True)

    def unmount(self):
        logger.info(f"Unmounting queue {self.path}")
        yt.unmount_table(self.path, sync=True)

    def write(self):
        logger.info(f"Writing to the queue {self.path}")
        rows = [{"value": RSG.generate(1024), "$tablet_index": random.choice(range(0, self.tablet_count))} for _ in range(10)]
        data_rows = []

        new_written_row_count = [0] * self.tablet_count
        for row in rows:
            tablet_index = row["$tablet_index"]
            row_index = self.written_row_count[tablet_index] + new_written_row_count[tablet_index]
            data_rows += [{"value": row["value"], "tablet_index": tablet_index, "row_index": row_index}]
            new_written_row_count[row["$tablet_index"]] += 1

        with yt.Transaction(type="tablet"):
            yt.insert_rows(self.path, rows)
            yt.insert_rows(self.data_path, data_rows)

        for tablet_index, row_count in enumerate(new_written_row_count):
            self.written_row_count[tablet_index] += row_count

        def check_written():
            tablet_infos = yt.get_tablet_infos(self.path, list(range(self.tablet_count)))["tablets"]
            for tablet_index in range(self.tablet_count):
                if tablet_infos[tablet_index]["total_row_count"] != self.written_row_count[tablet_index]:
                    return False
            return True

        wait(check_written)

    def flush(self):
        logger.info(f"Flushing queue {self.path}")
        if self.mounted:
            yt.freeze_table(self.path, sync=True)
            yt.unfreeze_table(self.path, sync=True)

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

            expected_rows = []
            while True:
                rows = list(yt.select_rows(f"select row_index, value from [{self.data_path}] where tablet_index = {tablet_index} order by row_index offset {len(expected_rows)} limit 100"))
                if len(rows) == 0:
                    break
                expected_rows += rows

            if len(actual_rows) != len(expected_rows):
                raise YtError(f"Data table {self.data_path} contains {len(expected_rows)} rows for tablet {tablet_index} but queue {self.path} contains {len(actual_rows)} rows")

            for expected_row, actual_row in zip(expected_rows, actual_rows):
                if expected_row["value"] != actual_row["value"]:
                    raise YtError(f"Row with value '{expected_row["value"]}' was expected in the queue {self.path} in the tablet {tablet_index} but value '{actual_row["value"]}' was read")


class HunkStorage:
    def __init__(self, base_path, name, cell_tag=None, tablet_count=1):
        self.name = name
        self.path = f"{base_path}/{name}"
        self.mounted = False
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

    def mount(self):
        logger.info(f"Mounting hunk storage {self.path}")
        yt.mount_table(self.path, sync=True)

    def unmount(self):
        logger.info(f"Unmounting hunk storage {self.path}")
        yt.unmount_table(self.path, sync=True)

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
    queue.hunk_storage_mounted = hunk_storage.mounted
    hunk_storage.linked_queue_names.add(queue.name)


def unlink(queue, hunk_storage):
    logger.info(f"Unlinking hunk storage {hunk_storage.path} and {queue.path}")
    yt.remove(f"{queue.path}/@hunk_storage_id")
    yt.remount_table(queue.path)
    queue.hunk_storage_name = None
    queue.hunk_storage_mounted = False
    hunk_storage.linked_queue_names.remove(queue.name)


def is_unmounted_error(err):
    err_str = str(err)
    unmounted_substrings = ["No such tablet", "has no mounted tablets", "Unknown cell 0-0-0-0", "is not known"]
    return err.is_tablet_not_mounted() or any(s in err_str for s in unmounted_substrings)


def test_queue_and_hunk_storage(base_path, spec, attributes, args):
    logging.getLogger('Yt').setLevel(logging.DEBUG)

    yt.config["backend"] = "rpc"

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
            if random.random() < spec.queue_and_hunk_storage.unmount_probability:
                queue.unmount()
            else:
                queue.mount()

        for hunk_storage in hunk_storages.values():
            if random.random() < spec.queue_and_hunk_storage.unmount_probability:
                # TODO(nadya73): Uncomment after YT-26227.
                """
                if len(hunk_storage.linked_queue_names) > 0:
                    for queue_name in hunk_storage.linked_queue_names:
                        queues[queue_name].unmount()
                hunk_storage.unmount()
                """
            else:
                hunk_storage.mount()


    def _write():
        for queue in queues.values():
            try:
                queue.write()
            except YtError as err:
                unmounted = not queue.mounted or (queue.hunk_storage_name and not hunk_storages[queue.hunk_storage_name].mounted)
                if unmounted and is_unmounted_error(err):
                    logger.info(f"Error was expected, queue or hunk_storage is unmounted")
                else:
                    raise err


    def _read():
        for queue in queues.values():
            unmounted = not queue.mounted or (queue.hunk_storage_name and not hunk_storages[queue.hunk_storage_name].mounted)
            try:
                queue.read_and_check()
            except YtError as err:
                if unmounted and is_unmounted_error(err):
                    logger.info(f"Error was expected, queue or hunk_storage is unmounted")
                else:
                    raise err


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
                queue.remove()
                removed_queue_names += [queue.name]
                removed_queue_count += 1

                if queue.hunk_storage_name:
                    hunk_storages[queue.hunk_storage_name].linked_queue_names.remove(queue.name)

        for queue_name in removed_queue_names:
            del queues[queue_name]

        # TODO(nadya73): Uncomment after YT-26227.
        """
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
        """


    for iteration in range(spec.size.iterations):
        logger.iteration = iteration

        _create_and_remove()
        _relink()
        _remount()

        for i in range(10):
            _write()
            _flush()
            _read()

        # TODO(nadya73): add copy/move when it will be fixed.
        # TODO(nadya73): add alter queue the the static tables and running maps in 25.4.
