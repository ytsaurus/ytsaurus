from .process_runner import process_runner
from .helpers import create_client
from .logger import logger
from .verify import verify_output, verify_tables_equal

import yt.wrapper as yt
from yt.wrapper.http_helpers import get_proxy_url

import random

def generate_selector(schema):
    columns = [c.name for c in schema.columns
        if c.type.str() != "any" and random.random() > 0.5]
    if len(columns) == 0:
        columns.append(schema.columns[0].name)
        assert schema.columns[0].type.str() != "any"
    scols = ",".join(columns)
    selector="{{{0}}}".format(scols)
    return selector, columns


class MapreduceRunner(object):
    def __init__(self, schema, data_table, table, dump_table,
        result_table, spec, cluster=None):
        self.schema = schema
        self.data_table = data_table
        self.table = table
        self.dump_table = dump_table
        self.result_table = result_table
        self.spec = spec
        self.cluster = cluster or get_proxy_url()

    def run(self):
        # TODO: ordered
        key_columns = self.schema.get_key_column_names()
        operations = {
            "ordered_merge": lambda: self.merge("ordered", ".ordered_merge"),
            "unordered_merge": lambda: self.merge("unordered", ".unordered_merge"),
            "sorted_merge": lambda: self.merge("sorted", ".sorted_merge"),

            "ordered_map": lambda: self.map(True, ".ordered_map"),
            "unordered_map": lambda: self.map(False, ".unordered_map"),

            "sort": lambda: self.sort(key_columns, ".sort"),
            "sort_reversed": lambda: self.sort(key_columns[::-1], ".sort_reversed"),
            "sort_partial": lambda: self.sort(key_columns[:-1], ".sort_partial"),

            "reduce": lambda: self.reduce(key_columns, ".reduce"),
            "reduce_partial": lambda: self.reduce(key_columns[:-1], ".reduce_partial"),

            "map_reduce": lambda: self.map_reduce(key_columns, ".map_reduce"),
            "map_reduce_partial": lambda: self.map_reduce(key_columns[:-1], ".map_reduce_partial"),

            "extract_merge": lambda: self.extract_merge(".extract_merge")

            # TODO: other operation kinds
        }

        types = self.spec.mr_options.operation_types
        if "all" in types:
            types = operations.keys()
        types = set(types)

        # Filtering.
        if len(key_columns) <= 1:
            types.discard("sort_partial")
        if len(key_columns) <= 2:
            types.discard("reduce_partial")
            types.discard("map_reduce_partial")

        for type in sorted(types):
            operations[type]()

    def run_operation(self, suffix, callback):
        """
        Calls |callback| and validates its output.

        |callback| is expected to do identity transform from |self.table|
        to |self.dump_table| + |suffix|.
        """
        client = create_client(self.cluster)
        client.remove(self.dump_table + suffix, force=True)

        callback(client)

        verify_output(
            self.schema,
            self.data_table,
            self.dump_table + suffix,
            self.result_table + suffix,
            suffix[1:].replace("_", " "),
            client)

    @process_runner.run_in_process()
    def merge(self, mode, suffix):
        logger.info("Run %s merge" % (mode))
        def callback(client):
            client.run_merge(
                self.table,
                self.dump_table + suffix,
                mode=mode,
                spec={"title": "MR: {} merge".format(mode)})
        self.run_operation(suffix, callback)

    @process_runner.run_in_process()
    def map(self, ordered, suffix):
        logger.info("Run %s map", "ordered" if ordered else "unordered")

        def mapper(record):
            yield record
        def callback(client):
            client.run_map(
                mapper,
                self.table,
                self.dump_table + suffix,
                ordered=ordered,
                spec={"title": "MR: {} map".format("ordered" if ordered else "unordered")})
        self.run_operation(suffix, callback)

    @process_runner.run_in_process()
    def sort(self, sort_by, suffix):
        logger.info("Run sort by %s", sort_by)
        def callback(client):
            client.run_sort(
                self.table,
                self.dump_table + suffix,
                sort_by=sort_by,
                spec={"title": "MR: sort"})
        self.run_operation(suffix, callback)

    @process_runner.run_in_process()
    def reduce(self, reduce_by, suffix):
        logger.info("Run reduce by %s", reduce_by)
        def callback(client):
            def reducer(key, records):
                for r in records:
                    yield r
            client.run_reduce(
                reducer,
                self.table,
                self.dump_table + suffix,
                reduce_by=reduce_by,
                spec={"title": "MR: reduce"})
        self.run_operation(suffix, callback)

    @process_runner.run_in_process()
    def map_reduce(self, reduce_by, suffix):
        logger.info("Run map-reduce by %s", reduce_by)
        def callback(client):
            def mapper(record):
                yield record
            def reducer(key, records):
                for r in records:
                    yield r
            client.run_map_reduce(
                mapper,
                reducer,
                self.table,
                self.dump_table + suffix,
                reduce_by=reduce_by,
                spec={"title": "MR: map-reduce"})
        self.run_operation(suffix, callback)

    def run_extract_operation(self, suffix, callback, selector, columns):
        client = create_client(self.cluster)
        dump_static = self.dump_table + suffix + ".static"
        dump_dynamic = self.dump_table + suffix + ".dynamic"
        client.remove(dump_static, force=True)
        client.remove(dump_dynamic, force=True)

        static_op = client.run_merge(
            self.data_table + selector,
            dump_static,
            mode="unordered",
            sync=False,
            spec={"title": "MR: extract merge from reference table"})
        callback(client)
        static_op.wait()

        verify_tables_equal(
            dump_static, dump_dynamic, self.result_table + suffix, columns, client=client)
        client.remove(dump_static)
        client.remove(dump_dynamic)

    @process_runner.run_in_process()
    def extract_merge(self, suffix):
        selector, columns = generate_selector(self.schema)
        logger.info("Run unordered merge with selector {}".format(selector))
        def callback(client):
            client.run_merge(
                self.table + selector,
                self.dump_table + suffix + ".dynamic",
                mode="unordered",
                spec={"title": "MR: extract merge from dynamic table"})
        self.run_extract_operation(suffix, callback, selector, columns)
