import argparse
import os
import sys
import time
import shutil

import yt.yson as yson
import yt.wrapper as yt
from yt.yson.yson_types import YsonEntity
from yp.local import YpInstance
from yt.wrapper import YtClient
from yt.wrapper.common import generate_uuid
from yt.wrapper.errors import YtOperationFailedError
from yt.wrapper.operation_commands import format_operation_stderrs

NODE_CONFIG = {
    "tablet_node": {
        "resource_limits": {
            "tablet_static_memory": 100 * 1024 * 1024,
        }
    }
}

def wait_result(callback, iter=300, sleep_backoff=0.3):
    for _ in xrange(iter - 1):
        try:
            return callback()
        except:
            time.sleep(sleep_backoff)
    return callback()

def dropna(row):
    for key in row.keys():
        if row[key] is None or isinstance(row[key], YsonEntity):
            row.pop(key)
    return row

def run_test(args):
    dump_meta = {}
    with open(os.path.join(args.dump_path, "dump_meta.yson")) as fin:
        dump_meta = yson.load(fin)

    assert dump_meta["version"] == 1

    yp_instance = YpInstance(args.sandbox_path,
                             local_yt_options=dict(enable_debug_logging=True, node_config=NODE_CONFIG),
                             enable_ssl=True,
                             db_version=1)
    yp_instance.prepare()

    yt_client = yp_instance.create_yt_client()
    old_resources = {}

    for table in dump_meta["tables"]:
        updates = []
        with open(os.path.join(args.dump_path, "tables", table)) as fin:
            for row in fin:
                data = dropna(dict(yson.loads(row)))
                updates.append(data)
                if table == "resources":
                    assert data["meta.id"] not in old_resources
                    old_resources[data["meta.id"]] = data
        yt_client.insert_rows(yt.ypath_join("//yp/db", table), updates)

    yp_instance.migrate_database(2, backup_path="//yp.backup")
    yp_instance.start()

    assert yt_client.exists("//yp/db/virtual_services")

    yp_client = yp_instance.create_client()
    new_resources = wait_result(lambda: yp_client.select_objects("resource", selectors=["/meta", "/spec", "/status"]))

    assert len(new_resources) == len(old_resources)
    for resource in new_resources:
        meta, spec, status = resource
        old_resource = old_resources[meta["id"]]
        assert meta["kind"] == old_resource["spec"]["kind"]
        assert spec[meta["kind"]]["total_capacity"] == old_resource["spec"]["total_capacity"]

        if meta["kind"] == "disk":
            assert spec["disk"]["storage_class"] == "hdd"
            assert spec["disk"]["supported_policies"] == ["quota"]

        for key in ["scheduled_allocations", "actual_allocations"]:
            new_allocs, old_allocs = status[key], old_resource.get("status.{}".format(key), YsonEntity)
            #assert (new_allocs is not YsonEntity and old_allocs is not YsonEntity) or (new_allocs is YsonEntity and old_allocs is YsonEntity)

            if not new_allocs and not new_allocs:
                continue

            old_allocs = filter(lambda alloc: "pod_id" in alloc and "capacity" in alloc, old_allocs)
            assert len(new_allocs) == len(old_allocs)

            for i in xrange(len(new_allocs)):
                assert new_allocs[i]["pod_id"] == old_allocs[i]["pod_id"]
                assert new_allocs[i][meta["kind"]]["capacity"] == old_allocs[i]["capacity"]

    pod_set_id = wait_result(lambda: yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": "default"}}))
    pod_id = wait_result(lambda: yp_client.create_object("pod", attributes={
            "meta": {
                "pod_set_id": pod_set_id
            },
            "spec": {
                "resource_requests": {
                    "vcpu_guarantee": 1
                },
                "enable_scheduling": True
            }
        }))

    assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "pending"
    time.sleep(10)
    assert yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/state"])[0] == "assigned"


def main(argv):
    def parse_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("--sandbox-path", help="Path to sandbox directory")
        parser.add_argument("--keep-temps", action="store_true", default=False)
        parser.add_argument("dump_path", help="Path to YP database dump")
        return parser.parse_args(argv)

    args = parse_args()
    if not args.sandbox_path:
        args.sandbox_path = "yp_" + generate_uuid()

    try:
        run_test(args)
    except YtOperationFailedError as error:
        print >>sys.stderr, error
        print >>sys.stderr
        print >>sys.stderr, format_operation_stderrs(error.attributes["stderrs"])

    if not args.keep_temps:
        shutil.rmtree(args.sandbox_path)

if __name__ == "__main__":
    main(sys.argv[1:])
