import argparse
import pprint
import sys

import yt.wrapper as yt


ERRORS=[]


def get_batch(client, paths):
    requests = [client.get(path) for key, path in paths]
    client.commit_batch()
    results=[]
    for (key, _), reply in zip(paths, requests):
        if reply.is_ok():
            results.append((key, reply.get_result()))
        else:
            ERRORS.append(reply.get_error())
    return results
    

def fetch_operation_simple_attr(client, operations, simple_attr, **kwargs):
    return get_batch(client, [(operation_id, simple_attr.format(operation_id)) for operation_id in operations])


def fetch_operation_output_chunks(client, operations, filter_account=None, **kwargs):
    output_tx = get_batch(client, [
        (operation_id, "//sys/operations/{}/@output_transaction_id".format(operation_id))
        for operation_id in operations
    ])

    resource_usage = get_batch(client, [
        (operation_id, "#{}/@resource_usage".format(tx_id))
        for operation_id, tx_id in output_tx
    ])
                                        
    def total_chunks(tx_resources):
        chunks = 0
        for account, usage in tx_resources.items():
            if filter_account is None or filter_account == account:
                chunks += usage["chunk_count"]
        return chunks

    return [(operation_id, total_chunks(tx_resources))
        for operation_id, tx_resources in resource_usage]


def fetch_input_disk_quota(client, operations, filter_account=None, **kwargs):
    input_tx = get_batch(client, [
         (operation_id, "//sys/operations/{}/@input_transaction_id".format(operation_id))
         for operation_id in operations])
                                     
    locked_nodes = get_batch(client, [
        (operation_id, "#{}/@locked_node_ids".format(tx_id))
        for operation_id, tx_id in input_tx])

    locked_objects = []
    for operation_id, locks in locked_nodes:
        for object_id in locks:
            locked_objects.append((operation_id, "#{}/@".format(object_id)))
    
    locked_usage = get_batch(client, locked_objects)

    operation_usage = {}
    for operation_id, object_attrs in locked_usage:
        if filter_account is None or filter_account == object_attrs["account"]:
            operation_usage[operation_id] = operation_usage.get(operation_id, 0) + object_attrs["resource_usage"]["disk_space"]
            
    return operation_usage.items()


OPERATION_TOP = {
    "job-count": lambda c, o: fetch_operation_simple_attr(c, o, "//sys/operations/{}/@progress/jobs/total"),
    "snapshot-size": lambda c, o: fetch_operation_simple_attr(c, o, "//sys/operations/{}/snapshot/@uncompressed_data_size"),
    "output-chunks": fetch_operation_output_chunks,
    "input-disk-quota": fetch_input_disk_quota,
}

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze running scheduler state.")
    parser.add_argument("-k", type=int, default=10, help="Number of operation to show (default: 10)")
    parser.add_argument("--proxy", help='Proxy alias')
    parser.add_argument("--top-by", choices=OPERATION_TOP.keys(), default="job-count")
    parser.add_argument("--in-account", help="Count resource usage only in this account")
    parser.add_argument("--show-errors", default=False, action="store_true")

    args = parser.parse_args()

    client = yt.YtClient(proxy=args.proxy)
    operations = client.list("//sys/operations")
    stats = OPERATION_TOP[args.top_by](
        client.create_batch_client(),
        operations,
        filter_account=args.in_account)

    if ERRORS:
        if not args.show_errors:
            print >>sys.stderr, "WARNING: {} errors, rerun with --show-errors see full error messages".format(len(ERRORS))
        else:
            for error in ERRORS:
                pprint.pprint(error, stream=sys.stderr)

    top = sorted(stats, key=lambda x: x[1], reverse=True)[:args.k]
    for operation_id, attr in top:
        link = "https://yt.yandex-team.ru/{}/#page=operation&mode=detail&id={}".format(args.proxy, operation_id)
        print "{:>20} {}".format(attr, link)
