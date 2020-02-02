#!/usr/bin/env python
# -*- coding: utf-8 -*-

from yp.client import YpClient, find_token
from yt.common import get_value, update
from yt.wrapper.cli_helpers import ParseStructuredArgument

import argparse


def main_impl(yp_client, arguments):
    attributes = {
        "meta": {
            "id": arguments.id,
            "inherit_acl": arguments.inherit_acl,
        },
        "spec": {
            "node_filter": "[/labels/segment]=\"{}\"".format(arguments.id),
        }
    }

    yp_client.create_object("node_segment", attributes=attributes)


def main(arguments):
    config = get_value(arguments.config, {})
    config = update(dict(token=find_token()), config)
    with YpClient(arguments.address, "grpc", config) as yp_client:
        main_impl(yp_client, arguments)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Create node segment")
    parser.add_argument("--address", required=True, help="Address of yp backend")
    parser.add_argument("--config", required=False, help="YP client config", action=ParseStructuredArgument)
    parser.add_argument("--id", required=True, help="Id of node segment")
    parser.add_argument("--inherit-acl", required=False, default=False, action="store_true", help="Inherit acl")

    return parser.parse_args()


if __name__ == "__main__":
    main(parse_arguments())
