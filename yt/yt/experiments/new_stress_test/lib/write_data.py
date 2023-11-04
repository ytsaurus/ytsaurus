from .job_base import JobBase
from .logger import logger
from .helpers import run_operation_and_wrap_error

import yt.wrapper as yt
import yt.yson as yson

import copy
from time import sleep
import random
import sys
import multiprocessing

MAX_ROWS_PER_TRANSACTION = 20000

@yt.aggregator
class WriterMapper(JobBase):
    def __init__(self, schema, table, aggregate, update, spec):
        super(WriterMapper, self).__init__(spec)
        self.aggregate = aggregate
        self.update = update
        self.table = table
    def __call__(self, records):
        client = self.create_client()
        params = {
            "path": self.table,
            "input_format": "yson",
            "aggregate": self.aggregate,
            "update": self.update,
        }

        rows = []
        for record in records:
            for k in record.keys():
                if k[0] == '@':
                    record.pop(k)
            rows.append(record)
            if len(rows) == MAX_ROWS_PER_TRANSACTION:
                self.make_request("insert_rows", params, self.prepare(rows), client)
                rows = []

        if rows:
            self.make_request("insert_rows", params, self.prepare(rows), client)

        if False:
            yield None

def write_data(schema, iter_table, table, aggregate, update, spec):
    logger.info("Write data into dynamic table")
    writing_completed = multiprocessing.Event()

    #  test_replicas_unmount_mount = False
    #  if test_replicas_unmount_mount:
    #      unmount_mount_replicas(yt.get(table + "/@replicas"), writing_completed)

    op_spec = {
        "job_count": spec.size.job_count,
        "title": "Write data into dynamic table",
    }
    if spec.get_write_user_slot_count() is not None:
        op_spec["scheduling_options_per_pool_tree"] = {
            "physical": {"resource_limits": {"user_slots": spec.get_write_user_slot_count()}}}
    with yt.TempTable() as tmp_table:
        op = yt.run_map(
            WriterMapper(schema, table, aggregate, update, spec),
            iter_table,
            tmp_table,
            spec=op_spec,
            sync=False)
        run_operation_and_wrap_error(op, "Write")
    writing_completed.set()

@yt.aggregator
class BulkInsertMapper():
    def __init__(self, schema, aggregate, update, use_unversioned_update_schema):
        self.schema = schema
        self.aggregate = aggregate
        self.update = update
        self.use_unversioned_update_schema = use_unversioned_update_schema
        if self.aggregate or self.update:
            assert self.use_unversioned_update_schema
    def __call__(self, records):
        print("Aggregate: {}, Update: {}, UnversionedUpdateSchema: {}".format(
                self.aggregate, self.update, self.use_unversioned_update_schema),
            file=sys.stderr)
        rows = []
        for record in records:
            for k in record.keys():
                if k[0] == '@':
                    record.pop(k)

            if not self.use_unversioned_update_schema:
                yield record
            else:
                output_row = {}
                for column in self.schema.columns:
                    if column.sort_order:
                        if column.name in record:
                            output_row[column.name] = record[column.name]
                    else:
                        flags = 0
                        if self.aggregate:
                            flags |= 2
                        if column.name in record:
                            output_row["$value:" + column.name] = record[column.name]
                        else:
                            if self.update:
                                flags |= 1
                        if flags != 0 or random.random() < 0.5:
                            output_row["$flags:" + column.name] = flags
                output_row["$change_type"] = 0
                yield output_row

@yt.aggregator
class ShardedMapper():
    def __init__(self, underlying_mapper, shard_count, shard_index):
        self.mapper = underlying_mapper
        self.shard_count = shard_count
        self.shard_index = shard_index
    def __call__(self, records):
        filtered = []
        print("Shard {} of {}".format(self.shard_index, self.shard_count), file=sys.stderr)
        total, good = 0, 0
        for record in records:
            h = hash(tuple(v for k, v in record.items() if k.startswith("k")))
            #  print(h, [v for k, v in record.items() if k.startswith("k")], file=sys.stderr)
            total += 1
            if h % self.shard_count == self.shard_index:
                good += 1
                filtered.append(record)
        for row in self.mapper(filtered):
            yield row
        print("Total {}, good {}".format(total, good), file=sys.stderr)

def write_data_bulk_insert(schema, iter_table, table, aggregate, update, shard_count,
                           args, schema_modification=None):
    logger.info("Bulk insert data into dynamic table")
    tracker = yt.OperationsTracker()
    tmp_tables = []
    for shard_index in range(shard_count):
        if random.random() < args.bulk_insert_probability:
            use_unversioned_update_schema = aggregate or update
            path = "<append=%true{}>{}".format(
                ";schema_modification=unversioned_update" if use_unversioned_update_schema else "",
                table)
            op = yt.run_map(
                ShardedMapper(
                    BulkInsertMapper(schema, aggregate, update, use_unversioned_update_schema),
                    shard_count,
                    shard_index),
                iter_table,
                path,
                spec={
                    "job_count": args.job_count,
                    "title": "Bulk insert (shard idx: {})".format(shard_index)},
                ordered=True,
                sync=False)
        else:
            tmp_table = yt.create_temp_table()
            tmp_tables.append(tmp_table)
            op = yt.run_map(
                ShardedMapper(
                    WriterMapper(schema, table, aggregate, update, args),
                    shard_count,
                    shard_index),
                iter_table,
                tmp_table,
                spec={
                    "job_count": args.job_count,
                    "title": "Regular insert (shard idx: {})".format(shard_index)},
                sync=False)
        tracker.add(op)
    tracker.wait_all()
    for tmp_table in tmp_tables:
        yt.remove(tmp_table, force=True)

##################################################################

@yt.reduce_aggregator
class OrderedWriterReducer(JobBase):
    def __init__(self, schema, table, tablet_size_table, insertion_batch_size, spec):
        super(OrderedWriterReducer, self).__init__(spec)
        self.tablet_size_table = tablet_size_table
        self.insertion_batch_size = insertion_batch_size
        self.table = table
        self.schema = schema

    def __call__(self, records):
        client = self.create_client()
        params = {
            "path": self.table,
            "input_format": "yson",
            "output_format": "yson",
        }
        tablet_size_params = copy.deepcopy(params)
        tablet_size_params["path"] = self.tablet_size_table

        tablet_index = None
        batch = []

        def do_flush():
            start_row_index = batch[0]["row_index"]

            with client.Transaction(type="tablet") as tx:
                params["transaction_id"] = tx.transaction_id
                tablet_size_params["transaction_id"] = tx.transaction_id

                tablet_info = next(yson.loads(self.make_request(
                    "lookup_rows", tablet_size_params, self.prepare([{"tablet_index": tablet_index}]), client,
                ), yson_type="list_fragment"))

                for row in batch:
                    if "tablet_index" not in self.schema.get_column_names():
                        row.pop("tablet_index")
                        row.pop("row_index")
                    row["$tablet_index"] = tablet_index

                self.make_request("insert_rows", params, self.prepare(batch), client)

                tablet_info["size"] += len(batch)
                self.make_request("insert_rows", tablet_size_params, self.prepare(tablet_info), client)
                print("Inserted {} rows".format(len(batch)), file=sys.stderr)
            print("Committed tx", file=sys.stderr)

        def flush():
            if not batch:
                return
            for attempt in range(1, self.retry_count + 1):
                try:
                    do_flush()
                    del batch[:]
                    return
                except yt.YtError as e:
                    print(e)
                    print("Attempt", attempt, file=sys.stderr)
                    print(str(error), file=sys.stderr)
                    print("\n" + "=" * 80 + "\n\n", file=sys.stderr)
                    sleep(random.randint(1, self.retry_interval))
            raise RuntimeError("Failed to write batch")

        for key, rows in records:
            row = next(rows)
            if row["tablet_index"] != tablet_index:
                flush()
                tablet_index = row["tablet_index"]
            batch.append(row)
            if len(batch) >= self.insertion_batch_size:
                flush()

        flush()

        if False:
            yield None

def write_ordered_data(
    schema, data_table, table, tablet_size_table, tablet_count, offsets, spec, args):

    logger.info("Write data into dynamic table")

    pivot_keys = [(tablet_index, offsets[tablet_index]) for tablet_index in range(1, tablet_count)]
    logger.info("Pivot keys for ordered writer: %s", pivot_keys)
    op_spec = {"reducer": {"memory_limit": 5*2**30}}
    if pivot_keys:
        op_spec["pivot_keys"] = pivot_keys
    else:
        op_spec["job_count"] = 1

    with yt.TempTable() as fake_output:
        yt.run_reduce(
            OrderedWriterReducer(schema, table, tablet_size_table, spec.ordered.insertion_batch_size, spec),
            data_table,
            fake_output,
            reduce_by=["tablet_index", "row_index"],
            spec=op_spec)

##################################################################

@yt.aggregator
class DeleterMapper(JobBase):
    def __init__(self, table, spec):
        super(DeleterMapper, self).__init__(spec)
        self.table = table

    def __call__(self, records):
        client = self.create_client()
        params = {
            "path": self.table,
            "input_format": "yson",
        }

        rows = []
        for record in records:
            for k in record.keys():
                if k[0] == '@':
                    record.pop(k)
            rows.append(record)
            if len(rows) == MAX_ROWS_PER_TRANSACTION:
                self.make_request("delete_rows", params, self.prepare(rows), client)
                rows = []

        if rows:
            self.make_request("delete_rows", params, self.prepare(rows), client)

        if False:
            yield None

def delete_data(iter_deletion_table, table, spec):
    logger.info("Deleting data from dynamic table")

    op_spec={
        "job_count": spec.size.job_count,
        "title": "Delete data from dynamic table"}
    if spec.get_write_user_slot_count() is not None:
        op_spec["scheduling_options_per_pool_tree"] = {
            "physical": {"resource_limits": {"user_slots": spec.get_write_user_slot_count()}}}

    with yt.TempTable() as tmp_table:
        op = yt.run_map(
            DeleterMapper(table, spec),
            iter_deletion_table,
            tmp_table,
            spec=op_spec,
            sync=False)
        run_operation_and_wrap_error(op, "Delete")
