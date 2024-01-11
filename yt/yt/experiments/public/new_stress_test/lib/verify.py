from .job_base import JobBase
from .helpers import create_client, equal_table_rows, run_operation_and_wrap_error
from .logger import logger

import yt.wrapper as yt
import yt.yson as yson

import os
import traceback

##################################################################

class VerifierReducer(JobBase):
    def __init__(self, schema):
        self.schema = schema
    def __call__(self, key, records):
        rows = [[], []]
        for r in records:
            rows[r["@table_index"]].append(r)
        if len(rows[0]) > 1 or len(rows[1]) > 1:
            for i in [0, 1]:
                rows[i] = {"variant%s" % j : rows[i][j] for j in range(len(rows[i]))}
            yield {"expected": rows[0], "actual": rows[1]}
        else:
            for i in [0, 1]:
                rows[i] = rows[i][0] if len(rows[i]) == 1 else None
        if not equal_table_rows(self.schema.get_column_names(), rows[0], rows[1]):
            yield {"expected": rows[0], "actual": rows[1]}

def verify_output(schema, data_table, dump_table, result_table, title="operation", client=None):
    logger.info("Verify output")
    key_columns = schema.get_key_column_names()

    if not client:
        client = create_client()
    client.create("table", result_table, force=True)
    client.run_sort(dump_table, sort_by=key_columns, spec={"title": "Sort before output verification"})
    client.set_attribute(result_table, "stack", traceback.format_stack())
    client.run_reduce(
        VerifierReducer(schema),
        [data_table, dump_table],
        result_table,
        reduce_by=key_columns,
        format=yt.YsonFormat(control_attributes_mode="row_fields"),
        spec={"title": f"Verify {title} output"})

    if client.get(result_table + "/@row_count") != 0:
        raise Exception(f"{title.capitalize()} failed: data mismatch")
    client.remove(dump_table)
    logger.info("Everything OK")

##################################################################

class EqualityVerificationReducer(JobBase):
    def __init__(self, columns):
        self.columns = columns
    def __call__(self, key, records):
        rows = [[], []]
        for r in records:
            rows[r["@table_index"]].append(r)
        if len(rows[0]) != len(rows[1]):
            yield {
                "row": (rows[0][0] if len(rows[0]) > 0 else None),
                "key": key,
                "expected": len(rows[0]),
                "actual": len(rows[1])}
        else:
            rows = [sorted(r, key=lambda x: x.items()) for r in rows]
            for i in range(len(rows[0])):
                if not equal_table_rows(self.columns, rows[0][i], rows[1][i]):
                    yield {"expected": rows[0][i], "actual": rows[1][i]}

def verify_tables_equal(
        expected_table, actual_table, result_table,
        columns_to_check, reduce_by=None, should_sort=True,
        timestamp=None, client=None):
    logger.info("Verify output")

    if reduce_by is None:
        reduce_by = columns_to_check
    if client is None:
        client = create_client()

    if should_sort:
        client.run_sort(
            expected_table,
            sort_by=reduce_by,
            spec={"title": "Sort expected table to verify equality"})
        client.run_sort(
            actual_table,
            sort_by=reduce_by,
            spec={"title": "Sort actual table to verify equality"})

    client.create("table", result_table, force=True)
    client.set_attribute(result_table, "stack", traceback.format_stack())

    def _get_ypath(path):
        if timestamp:
            return "<timestamp={}>{}".format(timestamp, path)
        else:
            return path

    client.run_reduce(
        EqualityVerificationReducer(columns_to_check),
        [_get_ypath(expected_table), _get_ypath(actual_table)],
        result_table,
        reduce_by=reduce_by,
        format=yt.YsonFormat(control_attributes_mode="row_fields"),
        spec={"title": "Verify equality"})

    if client.get(result_table + "/@row_count") > 0:
        raise Exception("Verification failed")
    logger.info("Everything OK")
