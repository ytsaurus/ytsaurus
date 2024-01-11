from .logger import logger
from .helpers import remove_existing, sync_flush_table, \
    mount_table, unmount_table, get_tablet_sizes
from .schema import TInt64, Schema
from .verify import verify_output
from .table_creation import create_dynamic_table
from .create_data import create_ordered_data
from .write_data import write_ordered_data
from .select import verify_select
from .process_runner import process_runner
from .reshard import reshard_table

import yt.wrapper as yt
import copy
import random
import time

class Registry(object):
    def __init__(self, base):
        self.base = base
        self.data = base + ".data"
        self.iter_data = None
        self.prev_data = base + ".prev_data"
        self.result = base + ".result"
        self.dump = base + ".dump"
        self.tablet_size = base + ".tablet_size"

    def make_iter_tables(self, iteration):
        self.iter_data = self.base + ".iter.{}".format(iteration)

def create_tablet_size_table(table, tablet_count):
    logger.info("Create tablet size table")
    if not yt.exists(table):
        attributes = {
            "dynamic": True,
            "schema": [
                {"name": "tablet_index", "type": "uint64", "sort_order": "ascending"},
                {"name": "size", "type": "uint64"},
            ],
        }
        yt.create("table", table, attributes=attributes)

    try:
        yt.mount_table(table, sync=True)
    except:
        time.sleep(5)
        yt.mount_table(table, sync=True)

    yt.insert_rows(table, [{"tablet_index": tablet_index, "size": 0} for tablet_index in range(tablet_count)])

def get_tablet_chunk_list_ids(table):
    root_chunk_list_id = yt.get(table + "/@chunk_list_id")
    return yt.get("#{}/@child_ids".format(root_chunk_list_id))

def create_tables(registry, schema, attributes, spec, args):
    if not spec.testing.skip_generation:
        remove_existing([registry.data, registry.tablet_size], args.force)
    if not spec.testing.skip_write:
        remove_existing([registry.base], args.force)

    remove_existing([registry.result, registry.dump], args.force)
    yt.create("table", registry.result)

    # TODO: unless something?
    create_tablet_size_table(registry.tablet_size, spec.size.tablet_count)

    if not spec.testing.skip_write:
        create_dynamic_table(
            registry.base,
            schema,
            attributes,
            spec.size.tablet_count,
            sorted=False,
            dynamic=not spec.prepare_table_via_alter,
            spec=spec)

def check_tablet_sizes(table, tablet_size_table, tablet_count):
    logger.info("Checking tablet sizes")
    expected_sizes = get_tablet_sizes(tablet_size_table, tablet_count)
    tablet_chunk_list_ids = get_tablet_chunk_list_ids(table)

    for tablet_index in range(tablet_count):
        chunk_row_count = 0
        for chunk_id in yt.get("#{}/@child_ids".format(tablet_chunk_list_ids[tablet_index])):
            chunk_row_count += yt.get("#{}/@row_count".format(chunk_id))

        assert expected_sizes[tablet_index] == chunk_row_count, \
            "Chunk sizes mismatch for tablet {}: expected row count {}, actual row count {}, ".format(
                tablet_index,
                expected_sizes[tablet_index],
                chunk_row_count)

def check_trimmed_rows(table, tablet_trimmed_row_count, tablet_count):
    logger.info("Checking trimmed rows")
    for tablet_index in range(tablet_count):
        actual_trimmed_row_count = yt.get("{}/@tablets/{}/trimmed_row_count".format(
            table, tablet_index))
        assert tablet_trimmed_row_count[tablet_index] == actual_trimmed_row_count, \
            "Trimmed row count check failed for tablet {}: expected {}, actual {}".format(
                tablet_index,
                tablet_trimmed_row_count[tablet_index],
                actual_trimmed_row_count)

def trim_and_update(registry, tablet_trimmed_row_count, tablet_count):
    logger.info("Trimming first chunk of each tablet")

    expected_tablet_sizes = get_tablet_sizes(registry.tablet_size, tablet_count)

    tablet_chunk_list_ids = get_tablet_chunk_list_ids(registry.base)
    current_row_count = 0

    for tablet_index in range(tablet_count):
        # TODO: filter out dynamic stores.
        chunk_ids = yt.get("#{}/@child_ids".format(tablet_chunk_list_ids[tablet_index]))
        if len(chunk_ids) == 0:
            assert expected_tablet_sizes[tablet_index] == 0
            # tablet_trimmed_row_count is unchanged.
            continue

        first_chunk_size = yt.get("#{}/@row_count".format(chunk_ids[0]))
        logger.info("Trimming {} out of {} rows from tablet {} (current_row_count: {})".format(
            first_chunk_size, expected_tablet_sizes[tablet_index], tablet_index, current_row_count))

        tablet_trimmed_row_count[tablet_index] += first_chunk_size
        yt.trim_rows(registry.base, tablet_index, tablet_trimmed_row_count[tablet_index])

        def check():
            actual = yt.get("{}/@tablets/{}/trimmed_row_count".format(registry.base, tablet_index))
            expected = tablet_trimmed_row_count[tablet_index]
            return expected == actual
        logger.info("Waiting for trimmed row count to be updated")
        for i in range(150):
            if check():
                break
            time.sleep(1)
        else:
            raise RuntimeError("Failed to wait for trimmed row count update")

        new_size = expected_tablet_sizes[tablet_index] - first_chunk_size
        yt.insert_rows(
            registry.tablet_size,
            [{"tablet_index": tablet_index, "size": new_size}])
        yt.run_erase(registry.data + "[#{}:#{}]".format(
            current_row_count, current_row_count + first_chunk_size))
        current_row_count += expected_tablet_sizes[tablet_index] - first_chunk_size

def add_data(
    schema, data_table_schema, registry, tablet_count,
    tablet_trimmed_row_count, spec, args
):
    tablet_sizes = get_tablet_sizes(registry.tablet_size, tablet_count)
    offsets = [x + y for x, y in zip(tablet_trimmed_row_count, tablet_sizes)]

    if not spec.testing.skip_generation:
        create_ordered_data(
            data_table_schema,
            registry.iter_data,
            tablet_count,
            offsets,
            spec)

        yt.run_merge([registry.data, registry.iter_data], registry.data, mode="sorted")

    # XXX: do aggregate and update make any sense in insert_rows?
    if not spec.testing.skip_write:
        write_ordered_data(
            schema,
            registry.iter_data,
            registry.base,
            registry.tablet_size,
            tablet_count,
            offsets,
            spec,
            args)

# XXX: revisit
class UpdateIndexesMapper():
    def __init__(self, tablet_count, partial_sums, tablet_trimmed_row_count):
        self.tablet_count = tablet_count
        self.partial_sums = partial_sums
        self.tablet_trimmed_row_count = tablet_trimmed_row_count
    def __call__(self, record):
        if record["tablet_index"] >= self.tablet_count:
            record["row_index"] += self.partial_sums[record["tablet_index"] - self.tablet_count + 1] + \
                self.tablet_trimmed_row_count[self.tablet_count - 1] - \
                self.tablet_trimmed_row_count[record["tablet_index"]]
            record["tablet_index"] = self.tablet_count - 1
        yield record

def update_data_indexes(data_table, tablet_size_table, tablet_count, tablet_trimmed_row_count,
                        new_tablet_count):
# XXX: revisit
    logger.info("Update row_index and tablet_index columns in data table")
    if tablet_count > new_tablet_count:
        tablet_sizes = get_tablet_sizes(tablet_size_table, tablet_count, new_tablet_count - 1)

        partial_sums = [0]
        for size in tablet_sizes:
            partial_sums.append(partial_sums[-1] + size)
        yt.insert_rows(tablet_size_table, [{"tablet_index": new_tablet_count - 1, "size": partial_sums[-1]}])

        logger.info("Cumulative sums on sizes of resharded tablets: " + " ".join(list(map(str, partial_sums))))

        yt.run_map(
            UpdateIndexesMapper(new_tablet_count, partial_sums, tablet_trimmed_row_count),
            data_table,
            data_table,
            ordered=True,
        )

    elif tablet_count < new_tablet_count:
        yt.insert_rows(tablet_size_table, [
            {"tablet_index": tablet_index, "size": 0} for tablet_index in range(tablet_count, new_tablet_count)
        ])

def do_reshard(registry, current_tablet_count, new_tablet_count,
    tablet_trimmed_row_count, spec
):
    logger.info("Resharding table %s into %s tablets", registry.base, new_tablet_count)
    unmount_table(registry.base)
    yt.reshard_table(registry.base, tablet_count=new_tablet_count, sync=True)
    mount_table(registry.base)

    update_data_indexes(
        registry.data,
        registry.tablet_size,
        current_tablet_count,
        tablet_trimmed_row_count,
        new_tablet_count)

    for tablet_index in range(new_tablet_count, current_tablet_count):
        tablet_trimmed_row_count[tablet_index] = 0

def test_ordered_tables(base_path, spec, attributes, args):
    table_path = base_path + "/ordered_table"
    attributes = copy.deepcopy(attributes)

    # XXX
    attributes.pop("chunk_format", None)

    registry = Registry(table_path)
    schema = Schema(sorted=False, spec=spec)

    logger.info("Schema data weight is %s", schema.data_weight())

    assert not spec.replicas

    create_tables(registry, schema, attributes, spec, args)

    tablet_trimmed_row_count = [0] * spec.size.tablet_count
    current_tablet_count = spec.size.tablet_count

    data_table_schema = schema.with_named_columns(
        ["tablet_index", "row_index"], [TInt64(), TInt64()], sort_order="ascending",
    )
    if not spec.testing.skip_generation:
        yt.create("table", registry.data, attributes={"schema": data_table_schema.yson()})

    spec.write_user_slots_per_node = 30
    spec.read_user_slots_per_node = 50

    for iteration in range(spec.size.iterations):
        logger.iteration = iteration

        registry.make_iter_tables(iteration)

        add_data(
            schema,
            data_table_schema,
            registry,
            current_tablet_count,
            tablet_trimmed_row_count,
            spec,
            args)

        sync_flush_table(registry.base)

        if spec.ordered.trim:
            trim_and_update(registry, tablet_trimmed_row_count, current_tablet_count)

        if not spec.testing.skip_verify:
            verify_select(
                data_table_schema,
                registry.data,
                registry.base,
                registry.dump + ".select",
                registry.result + ".select",
                ["$tablet_index", "$row_index"],
                spec,
                args,
                data_key_columns=["tablet_index", "row_index"])
            process_runner.join_processes()

        if spec.reshard:
            new_tablet_count = random.randint(1, spec.size.tablet_count)
            do_reshard(
                registry,
                current_tablet_count,
                new_tablet_count,
                tablet_trimmed_row_count,
                spec)
            current_tablet_count = new_tablet_count

        logger.info("Checking stuff for %s", registry.base)
        check_tablet_sizes(registry.base, registry.tablet_size, current_tablet_count)
        check_trimmed_rows(registry.base, tablet_trimmed_row_count, current_tablet_count)
