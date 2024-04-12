from .logger import logger
from .helpers import (
    remove_existing,
    sync_flush_table, unmount_table, mount_table,
    compact_chunk_views)
from .table_creation import create_dynamic_table, set_dynamic_table_attributes
from .create_data import create_keys, create_sorted_data, pick_keys_for_deletion
from .write_data import write_data, write_data_bulk_insert, delete_data
from .aggregate import aggregate_data
from .select import verify_select
from .group_by import verify_group_by
from .mapreduce import MapreduceRunner
from .lookup import verify_lookup
from .reshard import reshard_multiple_times
from .process_runner import process_runner
from .schema import Schema
from .secondary_index import (
    make_random_secondary_index_schema,
    verify_insert_secondary_index,
    verify_select_secondary_index,
    FilterMapper,
    SYSTEM_EMPTY_COLUMN_NAME)

import yt.wrapper as yt
import random
import copy


MAX_KEY_COLUMN_NUMBER = 64


class Registry(object):
    def __init__(self, base):
        self.base = base
        self.keys = base + ".keys"
        self.data = base + ".data"
        self.prev_data = base + ".prev_data"
        self.iter_data = None
        self.iter_deletion = None
        self.result = base + ".result"
        self.dump = base + ".dump"
        self.index = base + ".index"

    def create_iter_tables(self, schema, iteration, aggregate, update, force):
        suffix = str(iteration)
        if aggregate:
            suffix += "-aggr"
        if update:
            suffix += "-update"
        self.iter_data = self.base + f".iter.{suffix}"
        self.iter_deletion = self.base + f".iter.deletion.{suffix}"

        remove_existing([self.iter_data, self.iter_deletion], force)
        yt.create("table", self.iter_data, attributes={"schema": schema.yson_with_unique()})
        yt.create("table", self.iter_deletion, attributes={"schema": schema.yson_keys()})

"""

Iteration data flow:
    - generate keys                               -> .keys
    - generate data                         .keys -> .iter
    - pick deletion                         .keys -> .iter_del
    - move                                  .data -> .prev_data
    - aggregate      .prev_data, .iter, .iter_del -> .data

"""

def generate_iter_data(registry, schema, extra_key_count, aggregate, update, spec, force):
    # NB: Keys from |registry.keys| represent superset of actual keys in the table.
    # Some fraction of keys is used in |registry.iter_data| for (re)insertions.
    # Some fraction of keys is used in |registry.iter_deletion| for deletions.
    # All the keys are used for lookups and selects even if they are missing from the table.
    create_keys(schema, registry.keys, extra_key_count, spec, force)
    create_sorted_data(schema, registry.keys, registry.iter_data, spec)
    # NB: We actually do not delete keys from |registry.keys|
    # so rows will likely be reinserted upon next iterations.
    pick_keys_for_deletion(registry.keys, registry.iter_deletion, spec)

    yt.move(registry.data, registry.prev_data, force=True)
    aggregate_data(
        schema,
        registry.prev_data,
        registry.iter_data,
        registry.iter_deletion,
        registry.data,
        aggregate,
        update)
    yt.remove(registry.prev_data)

def write_to_dynamic_table(registry, attributes, schema, index_schema, aggregate, update, with_alter, force, spec):
    if with_alter:
        attributes = copy.deepcopy(attributes)
        assert yt.get(registry.base + "/@dynamic") == False

        # YT-18930

        if spec.chunk_format in ("table_versioned_simple", "table_versioned_slim", "table_versioned_indexed"):
            chunk_format = "table_unversioned_schemaless_horizontal"
            optimize_for = "lookup"
        else:
            chunk_format = "table_unversioned_columnar"
            optimize_for = "scan"
        yt.set(registry.base + "/@chunk_format", chunk_format)
        assert yt.get(registry.base + "/@optimize_for") == optimize_for

        yt.run_merge(
            registry.iter_data,
            registry.base,
            mode="ordered",
            spec={
                "job_io": {"table_writer": {
                    "desired_chunk_size": 100 * 2**20,
                    "block_size": 256 * 2**10}},
                "force_transform": True,
                "title": "Initial merge during table creation"})

        if spec.index:
            logger.info(f"Creating index table {registry.index}")

            remove_existing([registry.index], force)
            attributes["schema"] = index_schema.yson_with_unique()
            attributes["dynamic"] = False
            attributes["chunk_format"] = chunk_format
            attributes["optimize_for"] = optimize_for
            yt.create("table", registry.index, attributes=attributes)
            attributes["schema"] = [
                {"name": column.name, "type": column.type.str()}
                for column in index_schema.columns
                if column.name != SYSTEM_EMPTY_COLUMN_NAME
            ]
            with yt.TempTable(attributes=attributes) as tmp:
                yt.run_map(FilterMapper(index_schema.yson()), registry.base, tmp)
                yt.run_sort(tmp, registry.index, sort_by=index_schema.get_key_column_names())

            yt.alter_table(registry.index, dynamic=True)
            set_dynamic_table_attributes(registry.index, spec)
            yt.reshard_table(registry.index, index_schema.get_pivot_keys(), sync=True)

        yt.alter_table(registry.base, dynamic=True)
        set_dynamic_table_attributes(registry.base, spec)
        yt.reshard_table(registry.base, schema.get_pivot_keys(), sync=True)

        # YT-18930
        yt.set(registry.base + "/@chunk_format", spec.chunk_format)

        if spec.index:
            yt.set(registry.index + "/@chunk_format", spec.chunk_format)
            yt.create("secondary_index", attributes={
                "table_path": registry.base,
                "index_table_path": registry.index,
                "kind": spec.index.kind,
            })
            mount_table(registry.index)

        mount_table(registry.base)

    elif spec.sorted.write_policy == "insert_rows":
        write_data(schema, registry.iter_data, registry.base, aggregate, update, spec)
    elif spec.sorted.write_policy == "bulk_insert":
        # YYY
        assert False, "Not implemented"
        write_data_bulk_insert(schema, registry.iter_data, registry.base, aggregate, update, 5)
    elif spec.sorted.write_policy == "mixed":
        # YYY
        assert False, "Not implemented"
        write_data_bulk_insert(schema, registry.iter_data, registry.base, aggregate, update, 5)
    else:
        raise RuntimeError(f"Unknown sorted write policy \"{spec.sorted.write_policy}\"")

def create_tables(registry, schema, index_schema, attributes, spec, force):
    if not spec.testing.skip_generation:
        remove_existing([registry.keys, registry.data], force)
        yt.create("table", registry.keys, attributes={"schema": schema.yson_keys()})
        yt.create("table", registry.data, attributes={"schema": schema.yson_with_unique()})

    remove_existing([registry.result, registry.dump], force)
    yt.create("table", registry.result)

    if not spec.testing.skip_write:
        remove_existing([registry.base], force)
        create_dynamic_table(
            registry.base,
            schema,
            attributes,
            spec.size.tablet_count,
            sorted=True,
            dynamic=not spec.prepare_table_via_alter,
            skip_mount=bool(spec.index),
            spec=spec)

        if spec.index and not spec.prepare_table_via_alter:
            create_dynamic_table(
                registry.index,
                index_schema,
                attributes,
                None,
                sorted=True,
                dynamic=True,
                skip_mount=True,
                spec=spec)

            yt.create("secondary_index", attributes={
                "table_path": registry.base,
                "index_table_path": registry.index,
                "kind": spec.index.kind,
            })

            set_dynamic_table_attributes(registry.index, spec)
            yt.reshard_table(registry.index, index_schema.get_pivot_keys(), sync=True)

            logger.info(f"Mounting {registry.base} and {registry.index}")
            mount_table(registry.base)
            mount_table(registry.index)

def test_sorted_tables(base_path, spec, attributes, force):
    table_path = base_path + "/sorted_table"
    attributes = copy.deepcopy(attributes)
    registry = Registry(table_path)
    schema = Schema.from_spec(sorted=True, spec=spec)
    schema.create_pivot_keys(spec.size.tablet_count)
    if spec.index:
        index_schema = make_random_secondary_index_schema(schema)
        index_schema.create_pivot_keys(spec.size.tablet_count)
    else:
        index_schema = None

    logger.info("Schema data weight is %s", schema.data_weight())

    assert not spec.replicas

    create_tables(registry, schema, index_schema, attributes, spec, force)

    if spec.size.key_count is not None:
        key_count = spec.size.key_count
    else:
        key_count = spec.size.data_weight // schema.data_weight()
    logger.info("Key count is set to %s", key_count)

    spec.write_user_slots_per_node = 6
    spec.read_user_slots_per_node = 20

    for iteration in range(spec.size.iterations):
        logger.iteration = iteration

        if iteration == 0:
            aggregate = False
            update = True
        else:
            aggregate = random.random() < 0.9
            update = random.random() < 0.5

        # Prepare data in static tables.
        registry.create_iter_tables(schema, iteration, aggregate, update, force)
        if not spec.testing.skip_generation:
            extra_key_count = key_count if iteration == 0 else int(key_count * 0.2)
            generate_iter_data(
                registry,
                schema,
                extra_key_count,
                aggregate,
                update,
                spec,
                force)

        # Write (and delete) data into the dynamic table.
        if not spec.testing.skip_write:
            with_alter = iteration == 0 and spec.prepare_table_via_alter
            write_to_dynamic_table(
                registry, attributes, schema, index_schema, aggregate, update, with_alter, force, spec)
            delete_data(registry.iter_deletion, registry.base, spec)

        # Disturb the table with remote copy.
        if spec.remote_copy_to_itself:
            compact_chunk_views(registry.base)
            create_dynamic_table(
                registry.base + ".copy",
                schema,
                attributes,
                skip_mount=True,
                spec=spec,
            )
            yt.freeze_table(registry.base, sync=True)
            logger.info(
                "Run remote copy from %s to %s",
                registry.base,
                registry.base + ".copy")
            yt.run_remote_copy(
                registry.base,
                registry.base + ".copy",
                yt.config.config["proxy"]["url"],
                spec={
                    "title": "Remote copy to itself"
                })
            logger.info("Replace original table with the copied one")
            yt.move(registry.base + ".copy", registry.base, force=True)
            mount_table(registry.base)

        # Verify lookups.
        if not spec.testing.skip_verify and not spec.testing.skip_lookup:
            verify_lookup(schema, registry.keys, registry.data, registry.base, registry.result, spec)

        # Verify group by.
        if not spec.testing.skip_verify and not spec.testing.skip_group_by:
            verify_group_by(
                schema,
                registry.data,
                registry.base,
                registry.dump + ".group_by",
                spec)

        # Verify selects.
        if not spec.testing.skip_verify and not spec.testing.skip_select:
            key_columns = schema.get_key_column_names()
            verify_select(
                schema,
                registry.keys,
                registry.data,
                registry.base,
                registry.dump + ".select",
                registry.result + ".select",
                key_columns,
                spec)
            # In case of few key columns there may be too many rows for a certain values
            # of truncated key columns (e.g. if all of them are null).
            if len(key_columns) > 3:
                verify_select(
                    schema,
                    registry.keys,
                    registry.data,
                    registry.base,
                    registry.dump + ".partial_select",
                    registry.result + ".partial_select",
                    key_columns[:-1],
                    spec)

        # Verify secondary index
        if spec.index and not spec.testing.skip_verify:
            verify_insert_secondary_index(
                registry.base,
                schema,
                registry.index,
                index_schema,
                registry.dump + ".secondary_index",
                registry.index + ".insert_mismatch")
            if  not spec.testing.skip_select:
                verify_select_secondary_index(
                    registry.base,
                    schema,
                    registry.index,
                    index_schema.key_columns[0],
                    registry.index + ".select_mismatch")

        if not spec.skip_flush:
            sync_flush_table(registry.base)

        # TODO: disturbancies: mount-unmount, force compaction, reshard.

        # Run mapreduce operations.
        if spec.mapreduce:
            mr_runner = MapreduceRunner(
                schema,
                registry.data,
                registry.base,
                registry.dump,
                registry.result,
                spec)
            mr_runner.run()

        if not spec.testing.ignore_failed_mr:
            results = process_runner.join_processes()
            errors = [r for r in results if r is not None]
            if errors:
                raise yt.YtError("Some operations failed", inner_errors=errors)

        # All the stuff we do later is kinda useless otherwise.
        if iteration + 1 == spec.size.iterations:
            break

        if spec.reshard:
            unmount_table(registry.base)
            # TODO: spec
            reshard_multiple_times(registry.base, schema)
            if spec.index:
                unmount_table(registry.index)
                reshard_multiple_times(registry.index, index_schema)

        if spec.alter and len(schema.get_key_columns()) < MAX_KEY_COLUMN_NUMBER:
            logger.info("Altering table")

            extra_column = schema.add_key_column()
            if spec.index and extra_column:
                unmount_table(registry.index)
                index_schema.add_key_column(extra_column)
                yt.alter_table(registry.index, schema=index_schema.yson_with_unique())

            unmount_table(registry.base)
            yt.alter_table(registry.base, schema=schema.yson())
            yt.alter_table(registry.data, schema=schema.yson_with_unique())

            # YT-14130: shallow alter triggers a bug in reduce.
            yt.run_merge(
                registry.data,
                registry.data,
                spec={
                    "force_transform": True,
                    "title": "Alter data table"})

        mount_table(registry.base)
        if spec.index:
            mount_table(registry.index)
