from .logger import logger
from .table_creation import create_dynamic_table
from .job_base import JobBase
from .schema import Schema

import yt.wrapper as yt

import copy
import sys
import time


################################################################################


def ensure_table_exists(path, schema, do_create, args):
    if yt.exists(path):
        existing_schema = yt.get(path + "/@schema")

        def _filter_column(column):
            column = copy.deepcopy(column)
            for attr in list(column):
                if attr not in ("name", "type", "sort_order", "aggregate"):
                    del column[attr]
            return column

        def _check_schema_match():
            if len(existing_schema) != len(schema.yson()):
                return "Schema has invalid length: expected {}, got {}".format(
                    len(schema.yson()), len(existing_schema))
            for expected, actual in zip(schema.yson(), existing_schema):
                if _filter_column(expected) != _filter_column(actual):
                    return "Column schema mismatch: expected {}, got {}".format(
                        expected, actual)
            return None

        msg = _check_schema_match()
        if args.force:
            yt.remove(path)
        elif msg:
            raise RuntimeError(msg + ". Use --force to replace existing table")
        else:
            logger.info("Using exising table")
            return

    logger.info("Creating new table")
    do_create()


################################################################################


def create_table(path, schema, attributes, args, tablet_count=None, is_sorted=True):
    def do_create():
        create_dynamic_table(path, schema, attributes, tablet_count=tablet_count, sorted=is_sorted)
    ensure_table_exists(path, schema, do_create, args)


################################################################################


class RandomWriteMapper(JobBase):
    BATCH_SIZE = 10000
    REPORT_PERIOD = 100

    def __init__(self, schema, table, args):
        super(RandomWriteMapper, self).__init__(args)
        self.schema = schema
        self.table = table

    def __call__(self, record):
        client = self.create_client()
        iteration = 0

        params = {
            "path": self.table,
            "input_format": "yson",
        }

        while True:
            iteration += 1

            rows = [self.schema.generate_row() for i in range(self.BATCH_SIZE)]
            self.make_request("insert_rows", params, self.prepare(rows), client)

            if iteration % self.REPORT_PERIOD == 0:
                print("Written {} rows".format(self.BATCH_SIZE * iteration), file=sys.stderr)

        yield None


def run_stateless_writer(path, spec, attributes, args):
    attributes = copy.deepcopy(attributes)
    is_sorted = spec.table_type == "sorted"
    tablet_count = spec.size.tablet_count
    schema = Schema(sorted=is_sorted, spec=spec)
    schema.create_pivot_keys(tablet_count)

    spec.write_user_slots_per_node = 8

    assert spec.get_write_user_slot_count() is not None, \
        "Should specify --write-user-slot-count or --bundle-node-count " \
        "for stateless write mode"

    create_table(path, schema, attributes, args, tablet_count=tablet_count, is_sorted=is_sorted)

    with yt.TempTable() as fake_input:
        yt.write_table(fake_input, [{"a": "b"} for i in range(1000)])

        op_spec = {
            "title": "Eternal stateless writer",
            "job_count": 1000,
            "scheduling_options_per_pool_tree": {
                "physical": {
                    "resource_limits": {
                        "user_slots": spec.get_write_user_slot_count()}}}}
        with yt.TempTable() as fake_output:
            try:
                op = yt.run_map(
                    RandomWriteMapper(schema, path, args),
                    fake_input,
                    fake_output,
                    spec=op_spec,
                    sync=False)
                logger.info("Operation started. Use the following command to update "
                    "running job count:")
                print("yt update-op-parameters {} '{{scheduling_options_per_pool_tree="
                    "{{physical={{resource_limits={{user_slots={}}}}}}}}}'".format(
                        op.id, spec.get_write_user_slot_count()), file=sys.stderr)
                while True:
                    try:
                        yt.remount_table(path)
                    except Exception as ex:
                        print(ex)
                    time.sleep(5)
            except:
                op.abort()
                raise
