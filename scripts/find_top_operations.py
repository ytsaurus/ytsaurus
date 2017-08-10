import argparse
import pprint
import sys

import yt.wrapper as yt


class Operation(object):
    def __init__(self, operation_id, cluster, batch_client):
        self._batch_client = batch_client
        self._cluster = cluster

        self.operation_id = operation_id
        self.errors = []
        self._attrs = None
        self._snapshot_size = None
        self._output_resource_usage = None
        self._input_locked_nodes = None
        self._input_disk_usage = None

    def fetch_attrs(self):
        self._attrs = self._batch_client.get("//sys/operations/{}/@".format(self.operation_id))

    def fetch_snapshot_size(self):
        self._snapshot_size = self._batch_client.get("//sys/operations/{}/snapshot/@uncompressed_data_size".format(self.operation_id))

    def fetch_output_resource_usage(self):
        if not self._attrs.is_ok():
            self.errors.append(self._attrs.get_error())
            self._output_resource_usage = self.attrs
            return

        output_tx = self._attrs.get_result()["output_transaction_id"]
        self._output_resource_usage = self._batch_client.get("#{}/@resource_usage".format(output_tx))

    def fetch_input_locked_nodes(self):
        if not self._attrs.is_ok():
            self.errors.append(self._attrs.get_error())
            self._input_locked_nodes = self.attrs
            return

        input_tx = self._attrs.get_result()["input_transaction_id"]
        self._input_locked_nodes = self._batch_client.get("#{}/@locked_node_ids".format(input_tx))

    def fetch_input_disk_usage(self):
        if not self._input_locked_nodes.is_ok():
            self.errors.append(self._attrs.get_error())
            self._input_object_attrs = []
            return

        self._input_object_attrs = []
        for object_id in self._input_locked_nodes.get_result():
            self._input_object_attrs.append(self._batch_client.get("#{}/@".format(object_id)))

    def get_default_attrs(self):
        if not self._attrs.is_ok():
            self.errors.append(self._attrs.get_error())
            return None
        return {
            "operation_type": self._attrs.get_result()["operation_type"],
            "authenticated_user": self._attrs.get_result()["authenticated_user"],
            "pool": self._attrs.get_result()["pool"],
            "title": self._attrs.get_result()["spec"].get("title", "")
        }

    def get_job_count(self):
        if not self._attrs.is_ok():
            self.errors.append(self._attrs.get_error())
            return None

        progress = self._attrs.get_result().get("progress")
        if progress is not None and "jobs" in progress:
            return progress["jobs"]["total"]
        else:
            return None

    def get_slice_count(self):
        if not self._attrs.is_ok():
            self.errors.append(self._attrs.get_error())
            return None

        progress = self._attrs.get_result().get("progress")
        if progress is not None and "estimated_input_statistics" in progress and \
            "data_slice_count" in progress["estimated_input_statistics"]:
                return progress["estimated_input_statistics"]["data_slice_count"]
        else:
            return None

    def get_snapshot_size(self):
        if not self._snapshot_size.is_ok():
            self.errors.append(self._snapshot_size.get_error())
            return None

        return self._snapshot_size.get_result()

    def get_output_chunks(self, in_account=None):
        if not self._output_resource_usage.is_ok():
            self.errors.append(self._output_resource_usage.get_error())
            return None

        chunks = 0
        for account, usage in self._output_resource_usage.get_result().items():
            if in_account is None or in_account == account:
                chunks += usage["chunk_count"]
        return chunks

    def get_input_disk_usage(self, in_account=None):
        disk_space = 0
        for input_object in self._input_object_attrs:
            if not input_object.is_ok():
                self.errors.append(self._input_object.get_error())
                continue

            if in_account is not None and input_object.get_result()["account"] != in_account:
                continue

            disk_space += input_object.get_result()["resource_usage"]["disk_space"]
        return disk_space

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze running scheduler state.")
    parser.add_argument("-k", type=int, default=10, help="Number of operation to show (default: 10)")
    parser.add_argument("--proxy", help='Proxy alias')
    parser.add_argument("--top-by", choices=["job-count", "snapshot-size", "input-disk-usage", "output-chunks", "slice-count"], default="job-count")
    parser.add_argument("--in-account", help="Count resource usage only in this account")
    parser.add_argument("--show-errors", default=False, action="store_true")

    args = parser.parse_args()

    client = yt.YtClient(proxy=args.proxy)
    batch_client = client.create_batch_client()
    operations = [Operation(operation_id, args.proxy, batch_client) for operation_id in client.list("//sys/operations")]

    for op in operations:
        op.fetch_attrs()
    batch_client.commit_batch()

    if args.top_by == "job-count":
        report_operations(operations, args.k, lambda op: op.get_job_count(), args.top_by)
    elif args.top_by == "slice-count":
        report_operations(operations, args.k, lambda op: op.get_slice_count(), args.top_by)
    elif args.top_by == "snapshot-size":
        for op in operations:
            op.fetch_snapshot_size()
        batch_client.commit_batch()

        report_operations(operations, args.k, lambda op: op.get_snapshot_size(), args.top_by)
    elif args.top_by == "input-disk-usage":
        for op in operations:
            op.fetch_input_locked_nodes()
        batch_client.commit_batch()

        for op in operations:
            op.fetch_input_disk_usage()
        batch_client.commit_batch()

        report_operations(operations, args.k, lambda op: op.get_input_disk_usage(args.in_account), args.top_by)
    elif args.top_by == "output-chunks":
        for op in operations:
            op.fetch_output_resource_usage()
        batch_client.commit_batch()

        report_operations(operations, args.k, lambda op: op.get_output_chunks(args.in_account), args.top_by)

    if args.show_errors:
        for op in operations:
            if not op.errors:
                continue
            pprint.pprint(op.errors, stream=sys.stderr)
    else:
        num_errors = sum(len(op.errors) for op in operations)
        if num_errors > 0:
            print >>sys.stderr, "WARNING: {} errors, rerun with --show-errors see full error messages".format(num_errors)
