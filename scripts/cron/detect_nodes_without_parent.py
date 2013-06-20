#!/usr/bin/env python

import yt.wrapper as yt

import logging

def main():
    for node in yt.search("/", attributes=["parent_id"]):
        if node == "/":
            continue
        if "parent_id" not in node.attributes:
            logging.error("Node %s has no parent_id", node)

if __name__ == "__main__":
    main()
