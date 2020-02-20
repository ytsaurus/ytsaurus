import argparse
import difflib
import json
import jsondiff
import sys
import os

from yt.environment import arcadia_interop
from yt.wrapper import YtClient, ypath_join
from yt.wrapper.common import generate_uuid, filter_dict
from yp.local import YpInstance, INITIAL_DB_VERSION


def canonize_obj(data):
    def canonize(data):
        if isinstance(data, dict):
            data.pop("type_v2", None)
            for key in data:
                data[key] = canonize(data[key])

        if isinstance(data, list):
            for i in xrange(len(data)):
                data[i] = canonize(data[i])
            data = sorted(
                data,
                cmp=lambda a, b: cmp(json.dumps(a, sort_keys=True), json.dumps(b, sort_keys=True)),
            )

        return data

    data = canonize(data)

    for key, val in data.iteritems():
        if "user_attribute_keys" not in val:
            continue
        user_attribute_keys = val["user_attribute_keys"]

        if "forced_compaction_revision" in user_attribute_keys:
            user_attribute_keys.remove("forced_compaction_revision")

    return data


def get_schemas(client):
    COMPARE_KEYS = [
        "account",
        "compression_codec",
        "dynamic",
        "in_memory_mode",
        "key",
        "key_columns",
        "optimize_for",
        "schema",
        "schema_mode",
        "sorted",
        "sorted_by",
        "type",
        "user_attribute_keys",
        "version",
    ]

    result = {}
    for table in client.list("//yp/db", absolute=True):
        result[table] = filter_dict(
            lambda k, v: k in COMPARE_KEYS, client.get(ypath_join(table, "@"))
        )

    return canonize_obj(result)


def insert_environ_path(path):
    assert len(path) > 0
    tokens = set(os.environ.get("PATH", "").split(os.pathsep))
    if path not in tokens:
        os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])


def main(argv):
    def parse_args():
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--local-db-version",
            required=True,
            type=int,
            help="Migrate local yp instance to given version",
        )
        parser.add_argument(
            "--arcadia-root", required=True, help="Path to arcadia root, need for YT local",
        )
        parser.add_argument("--yp-address", required=True, help="YP cluster address")
        parser.add_argument("--sandbox-path", help="Path to sandbox directory")
        return parser.parse_args(argv)

    args = parse_args()

    uuid = generate_uuid()
    if not args.sandbox_path:
        args.sandbox_path = "sandbox_" + uuid

    os.makedirs(args.sandbox_path)
    for subdir in ["yp", "yt_build"]:
        os.makedirs(os.path.join(args.sandbox_path, subdir))

    insert_environ_path(
        os.path.realpath(
            arcadia_interop.prepare_yt_environment(
                args.sandbox_path + "/yt_build", arcadia_root=args.arcadia_root
            )
        )
    )

    remote_schemas = get_schemas(YtClient(args.yp_address))

    print "=========> Remote schemas:"
    print json.dumps(remote_schemas, indent=4)

    yp_instance = YpInstance(
        args.sandbox_path + "/yp", enable_ssl=True, db_version=INITIAL_DB_VERSION
    )
    yp_instance.prepare()
    yp_instance.migrate_database(args.local_db_version, backup=False)

    local_schemas = get_schemas(yp_instance.create_yt_client())

    print "=========> Local schemas:"
    print json.dumps(local_schemas, indent=4)

    print "=========> Schemas diff:"
    print jsondiff.diff(
        remote_schemas,
        local_schemas,
        dump=True,
        dumper=jsondiff.JsonDumper(indent=4),
        syntax="explicit",
    )


if __name__ == "__main__":
    main(sys.argv[1:])
