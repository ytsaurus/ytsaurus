from .job_base import JobBase
from .logger import logger
from .helpers import is_table_empty

import yt.wrapper as yt
from yt import yson

import sys
import random

##################################################################

def compare_keys(a, b):
    def compare_values(a,b):
        if a is None and b is None: return 0
        if a is None: return -1
        if b is None: return 1
        if a is b: return 0
        return -1 if a < b else 1
    for i in range(min(len(a), len(b))):
        res = compare_values(a[i], b[i])
        if res != 0: return res
    if len(a) == len(b): return 0
    return -1 if len(a) < len(b) else 1

##################################################################

class CreateKeysMapper():
    def __init__(self, schema):
        self.schema = schema
    def __call__(self, record):
        pivot_keys = self.schema.get_pivot_keys()
        count = [0] * len(pivot_keys)
        total_count = 0
        fail_count = 0
        max_bucket_size = (record["count"] + len(count) - 1) // len(count)
        while total_count < record["count"] and fail_count < 2 * record["count"]:
            key = self.schema.generate_key()
            flat_key = self.schema.flatten_key(key)
            index = self.search_index(flat_key, pivot_keys)
            if count[index] < max_bucket_size:
                count[index] += 1
                total_count += 1
                yield key
            else:
                fail_count += 1
    def search_index(self, key, pivot_keys):
        s = 0
        t = len(pivot_keys)
        while t - s > 1:
            m = (s + t) // 2
            if compare_keys(key, pivot_keys[m]) >= 0: s = m
            else: t = m
        return s

def create_keys(schema, key_table, extra_key_count, spec, force):
    """
    Generates about |extra_key_count| additional keys to the |key_table| according to
    the schema and removes duplicates. |key_table| may already contain some keys.

    New schema may be wider than the one of |key_table|, but at most one extra
    key column should be present.
    """

    logger.info("Generate random keys")

    assert yt.exists(key_table)

    if len(yt.get(key_table + "/@schema")) != len(schema.key_columns):
        logger.info("Alter existing key table")
        yt.alter_table(key_table, schema.yson_keys())
        # YT-14130: shallow alter triggers a bug in reduce.
        yt.run_merge(
            key_table,
            key_table,
            spec={
                "force_transform": True,
                "Title": "Alter key table"})

    new_keys = yt.create_temp_table()
    with yt.TempTable() as fake_input:
        if spec.size.data_job_count:
            job_count = spec.size.data_job_count
        else:
            data_weight_per_job = spec.size.data_weight_per_data_job or 10 * 2**20
            job_count = extra_key_count * schema.key_data_weight() // data_weight_per_job + 1

        rows = [{"count": (extra_key_count + job_count - 1) // job_count} for i in range(job_count)]
        yt.write_table(fake_input, rows, raw=False)
        yt.run_map(
            CreateKeysMapper(schema),
            fake_input,
            new_keys,
            spec={
                "job_count": job_count,
                "title": "Create new keys"})
    yt.run_sort(new_keys, sort_by=schema.get_key_column_names(), spec={"title": "Sort created keys"})

    new_key_table = "{}.new".format(key_table)
    output_schema = schema.yson_keys_with_unique()
    yt.create("table", new_key_table, attributes={"schema": output_schema}, force=force)

    def reducer(key, records):
        yield next(records)
        #  last_column_name = schema.get_key_column_names()[-1]
        #  records = sorted(list(records), key=lambda x: x.get(last_column_name, None))
        #  prev = None
        #  for record in records:
        #      if prev != record:
        #          yield record
        #          prev = record

    if spec.size.data_job_count:
        job_count = spec.size.data_job_count
    else:
        data_weight_per_job = spec.size.data_weight_per_data_job or 10 * 2**20
        total_key_count = extra_key_count + yt.get(key_table + "/@row_count")
        job_count = total_key_count * schema.key_data_weight() // data_weight_per_job + 1

    yt.run_reduce(
        reducer,
        [key_table, new_keys],
        new_key_table,
        reduce_by=schema.get_key_column_names(),
        spec={"job_count": job_count, "title": "Unique keys"})

    yt.move(new_key_table, key_table, force=True)
    yt.remove(new_keys)

##################################################################

@yt.aggregator
class CreateSortedDataMapper():
    def __init__(self, schema, insertion_probability):
        self.schema = schema
        self.insertion_probability = insertion_probability
    def __call__(self, records):
        for record in records:
            if random.random() > self.insertion_probability:
                continue
            for k in record.keys():
                if k[0] == '@':
                    print("Dropped key", k, file=sys.stderr)
                    record.pop(k)
            data = self.schema.generate_data()
            record.update(data)
            yield record

def create_sorted_data(schema, keys, dst, spec):
    """
    Generates random data for each key according to the schema with probability |insertion_probability|.
    Provided that |keys| table is sorted, |dst| would be sorted as well.

    |dst| table should be empty.
    """

    logger.info("Generate random data")

    assert is_table_empty(dst)

    row_count = yt.get(keys + "/@row_count")
    if spec.size.data_job_count:
        job_count = spec.size.data_job_count
    else:
        data_weight_per_job = spec.size.data_weight_per_data_job or 10 * 2**20
        job_count = row_count * schema.data_weight() // data_weight_per_job + 1

    yt.run_map(
        CreateSortedDataMapper(schema, spec.sorted.insertion_probability),
        keys,
        dst,
        spec={
            "job_count": job_count,
            "title": "Create data"},
        format=yt.YsonFormat(),
        ordered=True)

###################################################################

class CreateOrderedDataMapper():
    def __init__(self, schema, offsets):
        self.schema = schema
        self.offsets = list(offsets)
    def __call__(self, record):
        for row_index in range(record["count"]):
            row = self.schema.generate_data()
            tablet_index = record["tablet_index"]
            row["tablet_index"] = tablet_index
            row["row_index"] = (row_index + self.offsets[tablet_index])
            yield row

def create_ordered_data(schema, dst, tablet_count, offsets, spec):
    logger.info("Create random data for ordered table")
    with yt.TempTable() as fake_input:
        fake_input = yt.create_temp_table()
        rows = [
            {"tablet_index": tablet_index, "count": spec.ordered.rows_per_tablet}
            for tablet_index in range(tablet_count)]
        yt.write_table(fake_input, rows)

        # XXX
        yt.remove(dst, force=True)
        yt.create("table", dst, attributes={"schema": schema.yson()})

        yt.run_map(
            CreateOrderedDataMapper(schema, offsets),
            fake_input,
            dst,
            ordered=True,
            spec={
                "job_count": tablet_count,
                "title": "Create data for ordered table",
            },
        )

##################################################################

def pick_keys_for_deletion(key_table, iter_deletion_table, spec):
    logger.info("Picking some keys for deletion")

    assert is_table_empty(iter_deletion_table)

    yt.run_merge(
        key_table,
        iter_deletion_table,
        mode="ordered",
        spec={
            "job_io": {
                "table_reader": {
                    "sampling_rate": spec.sorted.deletion_probability,
                    "sampling_seed": spec.seed,
                },
            },
            "title": "Pick keys for deletion",
            "force_transform": True,
        }
    )

    logger.info("{} out of {} rows are picked for deletion".format(
        yt.get("{}/@row_count".format(iter_deletion_table)),
        yt.get("{}/@row_count".format(key_table))))
