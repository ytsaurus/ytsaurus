from .job_base import JobBase
from .process_runner import process_runner
from .helpers import create_client, run_operation_and_wrap_error
from .verify import verify_output
from .logger import logger

import yt.wrapper as yt
from yt.wrapper.http_helpers import get_proxy_url
import yt.yson as yson

MAX_ROWS_PER_REQUEST = 10000

@yt.reduce_aggregator
class SelectReducer(JobBase):
    def __init__(self, table, spec, key_columns, data_key_columns=None):
        super(SelectReducer, self).__init__(spec)
        self.key_columns = key_columns
        self.data_key_columns = data_key_columns or key_columns
        self.table = table
    def __call__(self, row_groups):
        client = self.create_client()

        keys = []

        def flush():
            if not keys:
                return

            query = "* from [%s] where (%s) in (%s)" % (
                self.table, ",".join(["[%s]" % column for column in self.key_columns]), ",".join(keys),
            )
            params = {
                "query": query,
                "output_format": "yson",
                "timeout": 120000,
            }
            data = self.make_request("select_rows", params, b"", client=client)
            result = yson.loads(data, yson_type="list_fragment")
            for row in result:
                if "$tablet_index" in row:
                    row["tablet_index"] = row["$tablet_index"]
                    row["row_index"] = row["$row_index"]
                yield row

            del keys[:]

        for key, rows in row_groups:
            if len(keys) >= MAX_ROWS_PER_REQUEST:
                for x in flush():
                    yield x
            keylist = ["null" if key.get(k, None) is None else yson.dumps(key[k]).decode() for k in self.data_key_columns]
            keys.append("(%s)" % (",".join(keylist)))

        for x in flush():
            yield x


@process_runner.run_in_process()
def verify_select(schema, key_table, data_table, table, dump_table, result_table, key_columns, spec, data_key_columns=None):
    logger.info("Verify select for key %s" % (key_columns))

    # XXX: revisit name data_key_columns

    def check_bool(schema, key_columns):
        yschema = schema.yson()
        for k in key_columns:
            for c in yschema:
                if c["name"] == k and c["type"] != "boolean":
                    return True
        return False

    if data_key_columns is None:
        data_key_columns = key_columns

    if not check_bool(schema, data_key_columns):
        logger.info("Disabled since all keys are boolean")
        return

    client = create_client(get_proxy_url(), yt.config.config)
    op_spec = {
        "job_count": spec.size.job_count,
        "title": "Verify select",
    }
    if spec.get_read_user_slot_count() is not None:
        op_spec["scheduling_options_per_pool_tree"] = {
            "physical": {"resource_limits": {"user_slots": spec.get_read_user_slot_count()}}}

    op = client.run_reduce(
        SelectReducer(table, spec, key_columns, data_key_columns),
        key_table,
        dump_table,
        reduce_by=key_columns,
        spec=op_spec,
        sync=False)
    run_operation_and_wrap_error(op, "Select")
    verify_output(schema, data_table, dump_table, result_table, "select", client)
