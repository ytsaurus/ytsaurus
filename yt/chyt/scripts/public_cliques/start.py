#!/usr/bin/python3

import argparse
import sys
import logging
import yt.clickhouse as chyt
from yt.common import update_inplace
import yt.yson as yson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname).1s  %(module)s:%(lineno)d  %(message)s"))
logger.addHandler(handler)


def main(ya_make_built=True):
    parser = argparse.ArgumentParser(description="Restart public (or its equivalent, for example, prestable) clique")
    parser.add_argument("-b", "--bin", default="//sys/clickhouse/bin/ytserver-clickhouse",
                        help="ytserver-clickhouse binary to use")
    parser.add_argument("-t", "--type", default="public", help="Clique type; one of 'public', 'prestable', 'datalens', "
                                                               "'prestable_datalens'")
    parser.add_argument("--no-graceful-preemption", action="store_true", help="Disable graceful preemption")
    parser.add_argument("--allow-regular-python", action="store_true",
                        help="NOT RECOMMENDED: try to run script with system python; normally you should run "
                             "ya make built binary")
    parser.add_argument("--yt-alloc-config", help="Translates to YT_ALLOC_CONFIG env for instances")
    parser.add_argument("--config", help="Arbitrary config patch")
    args = parser.parse_args()

    if not ya_make_built:
        if args.allow_regular_python:
            logger.warning("Running with system python; your yandex-yt library may miss some important "
                           "clickhouse features to run public cliques")
        else:
            logger.error("Running with system python is not recommended; if you still want to proceed, "
                         "specify --allow-regular-python")
            sys.exit(1)

    assert args.type in ("prestable_datalens", "prestable", "datalens", "public")

    alias = "*ch_" + args.type

    cpu_limit = 16

    acl = [
        {
            "subjects": ["users"],
            "permissions": ["read"],
            "action": "allow",
        }
    ]

    if args.type in ("prestable", "public"):
        # idm-group:29731 is a group corresponding to all staff robots taken from
        # //sys/clickhouse/acl_nodes/robots/@acl.
        acl.append({
            "subjects": ["idm-group:29731"],
            "permissions": ["read"],
            "action": "deny",
        })

    spec = {
        "acl": acl,
        "title": args.type.capitalize() + " clique",
        "pool": "chyt",
        "preemption_mode": "graceful" if not args.no_graceful_preemption else "normal",
    }

    clickhouse_config = {
        "engine": {
            "settings": {
                "max_execution_time": 6000 if "datalens" in args.type else 1800,
                "max_threads": cpu_limit,
            },
        },
    }

    if args.type in ("prestable_datalens", "datalens"):
        datalens_config_patch = {
            "yt": {
                "settings": {
                    "composite": {
                        "default_yson_format": "unescaped_pretty",
                    },
                },
            },
        }
        update_inplace(clickhouse_config, datalens_config_patch)

    if args.config:
        update_inplace(clickhouse_config, yson.loads(args.config))

    if args.yt_alloc_config:
        spec["tasks"] = {"instances": {"environment": {"YT_ALLOC_CONFIG": args.yt_alloc_config}}}

    chyt.start_clique(
        8 if args.type != "prestable" else 2,
        alias,
        cpu_limit=cpu_limit,
        enable_monitoring=True,
        enable_job_tables=True,
        cypress_geodata_path="//sys/clickhouse/geodata/geodata.tgz",
        cypress_ytserver_clickhouse_path=args.bin,
        spec=spec,
        clickhouse_config=clickhouse_config,
        memory_config={
        },
        abort_existing=True,
        dump_tables=True)


if __name__ == "__main__":
    main(ya_make_built=False)
