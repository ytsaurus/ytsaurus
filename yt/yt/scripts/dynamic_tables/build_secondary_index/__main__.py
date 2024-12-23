#!/usr/bin/env python3

from argparse import ArgumentParser
from json import loads
from os import environ

import logging

from yt.yt.scripts.dynamic_tables.build_secondary_index.lib.build import (
    FULL_SYNC,
    UNIQUE,
    UNFOLDING,
    build_secondary_index,
)


def main():
    logging.basicConfig(format="%(asctime)s %(levelname)s\t%(message)s", level=environ.get('LOGLEVEL', 'INFO').upper())

    parser = ArgumentParser()
    parser.add_argument("--proxy", type=str, required=True, help="YT cluster")
    parser.add_argument("--table", type=str, required=True, help="path to the indexed table")
    parser.add_argument("--index-table", type=str, required=True, help="path to the index table")
    parser.add_argument(
        "--kind",
        type=str,
        required=True,
        help="secondary index kind",
        default=FULL_SYNC, choices=[FULL_SYNC, UNFOLDING, UNIQUE])
    parser.add_argument("--predicate", type=str, required=False, help="secondary index predicate")
    parser.add_argument("--dry-run", type=bool, required=False, help="only validate schemas")
    parser.add_argument("--pool", type=str, required=False, help="pool for map-reduce operation")
    parser.add_argument("--pools", type=loads, required=False, help="dictionary {cluster:pool} for map-reduce operations")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--build-strictly",
        dest="online",
        default=None,
        action="store_false",
        help="build stricly bijective index. Indexed table will be unavailable for the entire runtime of the script")
    group.add_argument(
        "--build-online",
        dest="online",
        default=None,
        action="store_true",
        help="build injective index. Indexed table will be unavailable for a short period of time. "
        "Upon completion index table might have extra rows in it")
    args = parser.parse_args()

    build_secondary_index(
        args.proxy,
        args.table,
        args.index_table,
        args.kind,
        args.predicate,
        args.dry_run,
        args.online if args.online is not None else True,
        args.pool,
        args.pools or {})


if __name__ == "__main__":
    main()
