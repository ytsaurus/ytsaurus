from .job_base import JobBase
from .process_runner import process_runner
from .helpers import create_client, run_operation_and_wrap_error
from .verify import verify_output
from .logger import logger

import yt.wrapper as yt
from yt.wrapper.common import generate_uuid
from yt.wrapper.http_helpers import get_proxy_url
import yt.yson as yson

import sys


@yt.reduce_aggregator
class PullQueueConsumerReducer(JobBase):
    def __init__(self, queue_table, consumer_table, spec):
        super(PullQueueConsumerReducer, self).__init__(spec)
        self.queue_table = queue_table
        self.consumer_table = consumer_table

    def __call__(self, row_groups):
        client = self.create_client()

        def flush(tablet_index):
            offset = 0
            consumer_rows = list(client.select_rows(f"* FROM [{self.consumer_table}] WHERE partition_index = {tablet_index}"))
            if consumer_rows:
                offset = consumer_rows[0]["offset"]

            while True:
                trace_id = generate_uuid()
                params = {
                    "queue_path": self.queue_table,
                    "consumer_path": self.consumer_table,
                    "offset": offset,
                    "partition_index": tablet_index,
                    "output_format": "yson",
                    "timeout": 120000,
                    "trace_id": trace_id,
                }

                data = self.make_request("pull_queue_consumer", params, b"", client=client)
                result = yson.loads(data, yson_type="list_fragment")
                row_count = 0
                for row in result:
                    row["trace_id"] = trace_id
                    if "$tablet_index" in row:
                        row["tablet_index"] = row["$tablet_index"]
                        row["row_index"] = row["$row_index"]
                    row_count += 1
                    yield row

                if row_count == 0:
                    break

                advance_params = {
                    "queue_path": self.queue_table,
                    "consumer_path": self.consumer_table,
                    "new_offset": offset + row_count,
                    "prev_offset": offset,
                    "partition_index": tablet_index,
                    "output_format": "yson",
                    "timeout": 120000,
                    "trace_id": trace_id,
                }
                self.make_request("advance_queue_consumer", advance_params, b"", client=client)

                offset += row_count

        tablets = [key["tablet_index"] for key, rows in row_groups]

        for tablet_index in tablets:
            for x in flush(tablet_index):
                yield x


@process_runner.run_in_process()
def verify_pull_queue_consumer(schema, tablet_count, tablets_table, data_table, queue_table, consumer_table, dump_table, result_table, spec):
    logger.info(f"Verify pull_queue_consumer")

    tablets_key_columns = ["tablet_index"]

    client = create_client(get_proxy_url(), yt.config.config)
    op_spec = {
        "job_count": tablet_count,
        "title": "Verify pull_queue_consumer",
        "reducer": {},
    }

    if spec.network_project is not None:
        op_spec["reducer"]["network_project"] = spec.network_project

    if spec.get_read_user_slot_count() is not None:
        op_spec["scheduling_options_per_pool_tree"] = {
            "physical": {"resource_limits": {"user_slots": spec.get_read_user_slot_count()}}}

    op = client.run_reduce(
        PullQueueConsumerReducer(queue_table, consumer_table, spec),
        tablets_table,
        dump_table,
        reduce_by=tablets_key_columns,
        spec=op_spec,
        sync=False)
    run_operation_and_wrap_error(op, "PullQueueConsumer")
    verify_output(schema, data_table, dump_table, result_table, "pull_queue_consumer", client)
