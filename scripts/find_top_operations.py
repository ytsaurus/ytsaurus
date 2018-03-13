import argparse
import pprint
import sys

import yt.wrapper as yt

from yt.yson.yson_types import YsonEntity

class Operation(object):
    def __init__(self, operation_id, cluster, batch_client):
        self._batch_client = batch_client
        self._cluster = cluster

        self.operation_id = operation_id
        self.errors = []
        self._attrs = None
        self._snapshot_size = None
        self._orchid = None
        self._output_resource_usage = None
        self._debug_output_resource_usage = None
        self._output_chunks = None
        self._output_disk_space = None
        self._input_locked_nodes = None
        self._input_disk_usage = None

    def fetch_attrs(self):
        attrs = self._batch_client.get("//sys/operations/{}/@".format(self.operation_id))
        yield

        if attrs.is_ok():
            self._attrs = attrs.get_result()
        else:
            self.errors.append(attrs.get_error())

    def fetch_snapshot_size(self):
        snapshot_size = self._batch_client.get("//sys/operations/{}/snapshot/@uncompressed_data_size".format(self.operation_id))
        yield

        if snapshot_size.is_ok():
            self._snapshot_size = snapshot_size.get_result()
        else:
            self.errors.append(snapshot_size.get_error())

    def fetch_output_resource_usage(self, in_account=None):
        if self._attrs is None:
            yield
            return

        output_tx = self._attrs["output_transaction_id"]
        debug_output_tx = self._attrs["async_scheduler_transaction_id"]
        output_resource_usage = self._batch_client.get("#{}/@resource_usage".format(output_tx))
        debug_output_resource_usage = self._batch_client.get("#{}/@resource_usage".format(debug_output_tx))
        yield

        def process_resource_usage(usage):
            if not usage.is_ok():
                self.errors.append(usage.get_error())
                return None

            usage = usage.get_result()
            chunks = 0
            disk_space = 0
            for account, usage in usage.items():
                if in_account is None or in_account == account:
                    chunks += usage["chunk_count"]
                    disk_space += usage["disk_space"]

            self._output_chunks = (self._output_chunks or 0) + chunks
            self._output_disk_space = (self._output_disk_space or 0) + disk_space
            return usage

        self._output_resource_usage = process_resource_usage(output_resource_usage)
        self._debug_output_resource_usage = process_resource_usage(debug_output_resource_usage)

    def fetch_input_locked_nodes(self):
        if self._attrs is None:
            yield
            return

        input_tx = self._attrs["input_transaction_id"]
        input_locked_nodes = self._batch_client.get("#{}/@locked_node_ids".format(input_tx))
        yield

        if input_locked_nodes.is_ok():
            self._input_locked_nodes = input_locked_nodes.get_result()
        else:
            self.errors.append(input_locked_nodes.get_error())

    def fetch_input_disk_usage(self, in_account=None):
        if self._input_locked_nodes is None:
            yield
            return

        input_object_attrs = []
        for object_id in self._input_locked_nodes:
            input_object_attrs.append(self._batch_client.get("#{}/@".format(object_id)))

        yield

        disk_space = 0
        for input_object in input_object_attrs:
            if not input_object.is_ok():
                self.errors.append(input_object.get_error())
                continue

            if in_account is not None and input_object.get_result()["account"] != in_account:
                continue

            disk_space += input_object.get_result()["resource_usage"]["disk_space"]
        self._input_disk_usage = disk_space

    def fetch_orchid(self):
        orchid = self._batch_client.get("//sys/scheduler/orchid/scheduler/operations/{}".format(self.operation_id))
        yield
        if orchid.is_ok():
            self._orchid = orchid.get_result()
        else:
            self._orchid = None
            self.errors.append(orchid.get_error())
    
    def fetch_controller_memory_usage(self):
        controller_memory_usage = self._batch_client.get("//sys/scheduler/orchid/scheduler/operations/{}/controller_memory_usage".format(self.operation_id))
        yield
        if controller_memory_usage.is_ok():
            self._controller_memory_usage = controller_memory_usage.get_result()
            if self._controller_memory_usage == YsonEntity():
                self._controller_memory_usage  = None
        else:
            self._controller_memory_usage = None
            self.errors.append(controller_memory_usage.get_error())

    def get_default_attrs(self):
        if self._attrs is None:
            return None

        return {
            "operation_type": self._attrs["operation_type"],
            "authenticated_user": self._attrs["authenticated_user"],
            "pool": self._attrs.get("pool", "<unknown>"),
            "title": self._attrs["spec"].get("title", "")
        }

    def get_job_count(self):
        if self._attrs is None:
            return None

        progress = self._attrs.get("progress")
        if progress is not None and "jobs" in progress:
            return progress["jobs"]["total"]
        else:
            return None

    def get_slice_count(self):
        progress = self._attrs.get("progress")
        if progress is not None and "estimated_input_statistics" in progress and \
            "data_slice_count" in progress["estimated_input_statistics"]:
                return progress["estimated_input_statistics"]["data_slice_count"]
        else:
            return None

    def get_input_table_count(self):
        if not self._attrs.is_ok():
            self.errors.append(self._attrs.get_error())
            return None

        spec = self._attrs["spec"]
        if "input_table_paths" in spec:
            return len(spec["input_table_paths"])
        else:
            return 1

    def get_output_table_count(self):
        if not self._attrs.is_ok():
            self.errors.append(self._attrs.get_error())
            return None
        spec = self._attrs["spec"]
        if "output_table_paths" in spec:
            return len(spec["output_table_paths"])
        else:
            return 1

    def get_controller_memory_usage(self):
        if self._controller_memory_usage is None:
            return None
        return self._controller_memory_usage

    def get_snapshot_size(self):
        return self._snapshot_size

    def get_output_chunks(self, in_account=None):
        return self._output_chunks

    def get_output_disk_space(self):
        return self._output_disk_space

    def get_input_disk_usage(self):
        return self._input_disk_usage

    def get_url(self):
        return "https://yt.yandex-team.ru/{}/#page=operation&mode=detail&id={}".format(self._cluster, self.operation_id)


def report_operations(operations, top_k, key_field, key_name):
    candidates = []
    for op in operations:
        candidate = op.get_default_attrs()
        if candidate is None:
            continue

        key = key_field(op)
        if key is None:
            continue

        candidate[key_name] = key
        candidate["url"] = op.get_url()
        candidates.append(candidate)

    displayed = sorted(candidates, key=lambda x: x[key_name], reverse=True)[:top_k]

    fields = [key_name, "operation_type", "authenticated_user", "pool", "url"]
    displayed = [dict((field, field) for field in fields)] + displayed
    sizes = [max(len(str(op[field])) for op in displayed) for field in fields]
    fmt = " ".join("{" + field + ":" + (">" if field == key_name else "")  + str(size) + "}" for field, size in zip(fields, sizes))

    for op in displayed:
        print fmt.format(**op)


def fetch_batch(batch_client, operations, fetch):
    generators = []
    for op in operations:
        generators.append(fetch(op))
        next(generators[-1])
    batch_client.commit_batch()
    for gen in generators:
        try:
            next(gen)
        except StopIteration:
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze running scheduler state.")
    parser.add_argument("-k", type=int, default=10, help="Number of operation to show (default: 10)")
    parser.add_argument("--proxy", help='Proxy alias')

    choices = ["job-count", "snapshot-size", "input-disk-usage", "output-chunks", "slice-count", "output-disk-space", "controller-memory-usage"]
    parser.add_argument("--top-by", choices=choices, default="job-count")

    parser.add_argument("--in-account", help="Count resource usage only in this account")
    parser.add_argument("--show-errors", default=False, action="store_true")

    args = parser.parse_args()

    client = yt.YtClient(proxy=args.proxy)
    batch_client = client.create_batch_client()
    operations = [Operation(operation_id, args.proxy, batch_client) for operation_id in client.list("//sys/operations") if len(operation_id) > 2]

    fetch_batch(batch_client, operations, lambda op: op.fetch_attrs())

    if args.top_by == "job-count":
        report_operations(operations, args.k, lambda op: op.get_job_count(), args.top_by)
    elif args.top_by == "slice-count":
        report_operations(operations, args.k, lambda op: op.get_slice_count(), args.top_by)
    elif args.top_by == "input-table-count":
        report_operations(operations, args.k, lambda op: op.get_input_table_count(), args.top_by)
    elif args.top_by == "output-table-count":
        report_operations(operations, args.k, lambda op: op.get_output_table_count(), args.top_by)
    elif args.top_by == "snapshot-size":
        fetch_batch(batch_client, operations, lambda op: op.fetch_snapshot_size())

        report_operations(operations, args.k, lambda op: op.get_snapshot_size(), args.top_by)
    elif args.top_by == "input-disk-usage":
        fetch_batch(batch_client, operations, lambda op: op.fetch_input_locked_nodes())
        fetch_batch(batch_client, operations, lambda op: op.fetch_input_disk_usage(args.in_account))

        report_operations(operations, args.k, lambda op: op.get_input_disk_usage(), args.top_by)
    elif args.top_by in ("output-chunks", "output-disk-space"):
        fetch_batch(batch_client, operations, lambda op: op.fetch_output_resource_usage(args.in_account))

        if args.top_by == "output-chunks":
            report_operations(operations, args.k, lambda op: op.get_output_chunks(), args.top_by)
        else:
            report_operations(operations, args.k, lambda op: op.get_output_disk_space(), args.top_by)
    elif args.top_by == "controller-memory-usage":
        fetch_batch(batch_client, operations, lambda op: op.fetch_controller_memory_usage())

        report_operations(operations, args.k, lambda op: op.get_controller_memory_usage(), args.top_by)

    if args.show_errors:
        for op in operations:
            if not op.errors:
                continue
            pprint.pprint(op.errors, stream=sys.stderr)
    else:
        num_errors = sum(len(op.errors) for op in operations)
        if num_errors > 0:
            print >>sys.stderr, "WARNING: {} errors, rerun with --show-errors see full error messages".format(num_errors)
