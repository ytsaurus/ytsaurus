#!/usr/bin/python3

from yt.wrapper import config, YtClient

from yt.environment.init_queue_agent_state import create_tables

import argparse


def build_arguments_parser():
    parser = argparse.ArgumentParser(description="Create queue agent state tables")
    parser.add_argument("--root", type=str, default="//sys/queue_agents",
                        help="Root directory for state tables; default to //sys/queue_agents")
    parser.add_argument("--proxy", type=str, default=config["proxy"]["url"])
    parser.add_argument("--skip-queues", action="store_true", help="Do not create queue state table")
    parser.add_argument("--skip-consumers", action="store_true", help="Do not create consumer state table")
    return parser


def main():
    args = build_arguments_parser().parse_args()
    client = YtClient(proxy=args.proxy, token=config["token"])
    create_tables(client, args.root, skip_queues=args.skip_queues, skip_consumers=args.skip_consumers)


if __name__ == "__main__":
    main()
