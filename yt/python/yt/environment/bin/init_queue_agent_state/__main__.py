#!/usr/bin/python3

from yt.wrapper import config, YtClient

from yt.environment.init_queue_agent_state import create_tables, DEFAULT_ROOT, DEFAULT_REGISTRATION_TABLE_PATH

import argparse


def build_arguments_parser():
    parser = argparse.ArgumentParser(description="Create queue agent state tables")
    parser.add_argument("--root", type=str, default=DEFAULT_ROOT,
                        help=f"Root directory for state tables; defaults to {DEFAULT_ROOT}")
    parser.add_argument("--registration-table-path", type=str, default=DEFAULT_REGISTRATION_TABLE_PATH,
                        help=f"Path to table with queue consumer registrations; defaults to {DEFAULT_REGISTRATION_TABLE_PATH}")
    parser.add_argument("--proxy", type=str, default=config["proxy"]["url"])
    parser.add_argument("--skip-queues", action="store_true", help="Do not create queue state table")
    parser.add_argument("--skip-consumers", action="store_true", help="Do not create consumer state table")
    parser.add_argument("--skip-object-mapping", action="store_true", help="Do not create queue agent object mapping table")
    parser.add_argument("--create-registration-table", action="store_true", help="Create registration state table")
    return parser


def main():
    args = build_arguments_parser().parse_args()
    client = YtClient(proxy=args.proxy, token=config["token"])
    create_tables(client,
                  root=args.root,
                  registration_table_path=args.registration_table_path,
                  skip_queues=args.skip_queues,
                  skip_consumers=args.skip_consumers,
                  skip_object_mapping=args.skip_object_mapping,
                  create_registration_table=args.create_registration_table)


if __name__ == "__main__":
    main()
