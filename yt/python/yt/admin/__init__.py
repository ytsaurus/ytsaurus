from .fetch_cluster_logs import add_fetch_cluster_logs_parser
from .fetch_cluster_info import add_fetch_cluster_info_parser

import argparse


def add_admin_parsers(subparsers: argparse._SubParsersAction) -> None:
    add_fetch_cluster_logs_parser(subparsers)
    add_fetch_cluster_info_parser(subparsers)
