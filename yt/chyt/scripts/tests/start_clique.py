#!/usr/bin/python3

import argparse
import copy
import getpass

import yt.yson as yson
import yt.clickhouse as chyt
from yt.common import update_inplace


KIND_TO_PARAMS = {
    "correctness1": {"instance_count": 1, "cpu_limit": 4},
    "correctness2": {"instance_count": 2, "cpu_limit": 4},
    "performance": {"instance_count": 2, "cpu_limit": 4},
}

DEFAULT_BIN_PATH = "//sys/bin/ytserver-clickhouse/ytserver-clickhouse"

DEFAULT_SPEC = {"tasks": {"instances": {"set_container_cpu_limit": True}}}


def start_clique(kind, bin_path, tag=None, spec=None, clickhouse_config=None, dry_run=False):
    params = copy.deepcopy(KIND_TO_PARAMS[kind])
    params["cypress_ytserver_clickhouse_path"] = bin_path
    params["abort_existing"] = True
    params["spec"] = copy.deepcopy(DEFAULT_SPEC)
    if clickhouse_config is not None:
        params["clickhouse_config"] = yson.loads(clickhouse_config.encode("utf-8"))
    params["alias"] = "*" + getpass.getuser() + "_" + kind + ("_" + tag if tag else "")
    if spec:
        update_inplace(params["spec"], yson.loads(spec.encode("utf-8")))

    print("Running clique with following params:")
    print(yson.dumps(params, yson_format="pretty").decode("utf-8"))

    if not dry_run:
        chyt.start_clique(**params)

    return params["alias"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", required=True, choices=list(KIND_TO_PARAMS.keys()),
                        help="Kind of clique to run, possible choices are: {}".format(list(KIND_TO_PARAMS.keys())))
    parser.add_argument("--tag", help="Arbitrary lowercase_with_underscore tag to append to clique name")
    parser.add_argument("--spec", help="Spec patch in YSON")
    parser.add_argument("--clickhouse-config", help="Config patch")
    parser.add_argument("--bin-path", default=DEFAULT_BIN_PATH,
                        help="Cypress path to ytserver-clickhouse binary; defaults to {}".format(DEFAULT_BIN_PATH))
    parser.add_argument("--dry-run", action="store_true", help="Do not run clique, but only print its parameters")

    args = parser.parse_args()

    start_clique(**vars(args))


if __name__ == "__main__":
    main()
