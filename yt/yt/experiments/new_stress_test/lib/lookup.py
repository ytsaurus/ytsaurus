from .job_base import JobBase
from .logger import logger
from .helpers import equal_table_rows

import yt.wrapper as yt
import yt.yson as yson

import traceback

MAX_ROWS_PER_REQUEST = 1000

@yt.reduce_aggregator
class LookupReducer(JobBase):
    def __init__(self, schema, table, spec):
        super(LookupReducer, self).__init__(spec)
        self.schema = schema
        self.table = table
    def __call__(self, row_groups):
        client = self.create_client()

        keys_buffer = []
        rows_buffer = []

        def flush():
            assert len(keys_buffer) == len(rows_buffer)
            if not keys_buffer:
                return

            params = {
                "path": self.table,
                "input_format": "yson",
                "output_format": "yson",
                "keep_missing_rows": True,
            }

            data = self.make_request("lookup_rows", params, self.prepare(keys_buffer), client)
            results = yson.loads(data, yson_type="list_fragment")

            for expected in rows_buffer:
                actual = next(results)
                if not equal_table_rows(self.schema.get_column_names(), expected, actual):
                    yield {"expected": expected, "actual": actual}

            del keys_buffer[:]
            del rows_buffer[:]

        for key, rows in row_groups:
            if len(keys_buffer) >= MAX_ROWS_PER_REQUEST:
                for x in flush():
                    yield x

            rows = sorted(rows, key=lambda x: len(x))

            keys_buffer.append(key)
            if len(rows) == 1:
                # Key is not present in data table.
                rows_buffer.append(None)
            else:
                rows_buffer.append(rows[1])

        for x in flush():
            yield x


def verify_lookup(schema, key_table, data_table, table, result_table, spec):
    """
    Performs lookup_rows into |table| for each key from |key_table|.
    Results are compared with |data_table|.
    Incorrect results are stored into |result_table|.

    Note that this function cannot detect extra data in |table|.
    """

    logger.info("Verify lookup")
    yt.set(result_table + "/@stack", traceback.format_stack())

    op_spec = {
        "job_count": spec.size.job_count,
        "title": "Verify lookup",
    }
    if spec.get_read_user_slot_count() is not None:
        op_spec["scheduling_options_per_pool_tree"] = {
            "physical": {"resource_limits": {"user_slots": spec.get_read_user_slot_count()}}}

    yt.run_reduce(
        LookupReducer(schema, table, spec),
        [key_table, data_table],
        result_table,
        reduce_by=schema.get_key_column_names(),
        spec=op_spec)
    if yt.get(result_table + "/@row_count") != 0:
        raise Exception("Verification failed")
    logger.info("Everything OK")
