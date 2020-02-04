import argparse
import sys

from yp.local import get_db_version, backup_yp
from yp.db_manager import DbManager
from yp.data_model import EObjectType
from yt.wrapper import YtClient

def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--yt-proxy", required=True, help="YT cluster.")
    parser.add_argument("--yp-path", required=True, help="YP path.")
    parser.add_argument("--commit", action="store_true", help="Change DB if given.")
    return parser.parse_args(argv)

def modify_pod_set_id(row, pod_set_key, pod_sets_mapping):
    if row[pod_set_key] in pod_sets_mapping:
        row[pod_set_key] = pod_sets_mapping[row[pod_set_key]]
    return row

def main(argv):
    args = parse_args(argv)

    yt_client = YtClient(args.yt_proxy)
    if args.commit:
        backup_yp(yt_client, args.yp_path)

    db_manager = DbManager(yt_client, args.yp_path, version=get_db_version(yt_client, args.yp_path))

    pod_sets_mapping = {}
    for row in yt_client.select_rows("[meta.id], [labels] from [{}]".format(db_manager.get_table_path("pod_sets"))):
        if row.get("labels", {}).get("deploy_engine", "") != "YP_LITE":
            continue
        if row["meta.id"] == row["labels"]["nanny_service_id"]:
            continue
        pod_sets_mapping[row["meta.id"]] = row["labels"]["nanny_service_id"]

    for pod_set_id, nanny_service_id in pod_sets_mapping.iteritems():
        print "pod_set {} will be renamed to {}".format(pod_set_id, nanny_service_id)

    if not args.commit:
        return

    def modify_pod_sets(row):
        yield modify_pod_set_id(row, "meta.id", pod_sets_mapping)
    db_manager.run_map("pod_sets", modify_pod_sets)

    def modify_pods(row):
        yield modify_pod_set_id(row, "meta.pod_set_id", pod_sets_mapping)
    db_manager.run_map("pods", modify_pods)

    def modify_node_segment_to_pod_sets(row):
        yield modify_pod_set_id(row, "pod_set_id", pod_sets_mapping)
    db_manager.run_map("node_segment_to_pod_sets", modify_node_segment_to_pod_sets)

    def modify_account_to_pod_sets(row):
        yield modify_pod_set_id(row, "pod_set_id", pod_sets_mapping)
    db_manager.run_map("account_to_pod_sets", modify_account_to_pod_sets)

    def modify_parents(row):
        if row["object_type"] == EObjectType.Value("OT_POD"):
            yield modify_pod_set_id(row, "parent_id", pod_sets_mapping)
        else:
            yield row

    db_manager.finalize()

if __name__ == "__main__":
    main(sys.argv[1:])
