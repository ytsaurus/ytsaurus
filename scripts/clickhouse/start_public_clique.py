#!/usr/bin/python2.7

import argparse
import logging
import yt.wrapper as yt
import yt.yson as yson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname).1s  %(module)s:%(lineno)d  %(message)s"))
logger.addHandler(handler)

def main():
    parser = argparse.ArgumentParser(description="Restart public (or its equivalent, for example, prestable) clique")
    parser.add_argument("-b", "--bin", default="//sys/clickhouse/bin/ytserver-clickhouse", help="ytserver-clickhouse binary to use")
    parser.add_argument("-t", "--type", default="public", help="Clique type; for example 'public' or 'prestable'")
    parser.add_argument("-g", "--graceful-preemption", action="store_true", help="Enable graceful preemption")
    args = parser.parse_args()

    alias = "*ch_" + args.type

    yt.start_clickhouse_clique(
        16,
        cpu_limit=8,
        enable_monitoring=True,
        cypress_geodata_path="//sys/clickhouse/geodata/geodata.tgz",
        cypress_ytserver_clickhouse_path=args.bin,
        spec={
            "acl": [{
                "subjects": ["yandex"],
                "permissions": ["read"],
                "action": "allow"
            }],
            "title": args.type.capitalize() + " clique",
            "pool": "chyt",
            "preemption_mode": "graceful" if args.graceful_preemption else "normal",
            "core_table_path": "//sys/clickhouse/kolkhoz/core_table_" + args.type,
            "stderr_table_path": "//sys/clickhouse/kolkhoz/stderr_table_" + args.type,
        },
        operation_alias=alias,
        abort_existing=True,
        dump_tables=True)


if __name__ == "__main__": 
    main()

