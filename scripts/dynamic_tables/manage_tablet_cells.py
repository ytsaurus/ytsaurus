#!/usr/bin/env python

import yt.wrapper as yt
import yt.yson as yson

import argparse
import sys

def verify_ok(reqs, rsps):
    assert len(reqs) == len(rsps)
    errors = []
    for i in xrange(len(rsps)):
        if "error" in rsps[i]:
            errors.append({"request": reqs[i], "response": rsps[i]})
    if len(errors) > 0:
        raise Exception(str(errors))

def execute_batch(reqs, **kwargs):
    rsps = yt.execute_batch(reqs, **kwargs)
    verify_ok(reqs, rsps)
    return rsps

def get_tablet_cell_count_per_bundle():
    bundles = yt.list("//sys/tablet_cell_bundles")
    reqs = []
    for bundle in bundles:
        reqs.append({"command": "get", "parameters": {
            "path": "//sys/tablet_cell_bundles/{0}/@tablet_cell_count".format(bundle)}})
    rsps = execute_batch(reqs, concurrency=100)
    data = {}
    for i in range(len(rsps)):
        data[bundles[i]] = rsps[i]["output"]
    return data

def create_tablet_cells(data):
    reqs = []
    for bundle, count in data.iteritems():
        for i in xrange(count):
            reqs.append({"command": "create", "parameters": {
                "type": "tablet_cell", "attributes": {"tablet_cell_bundle": bundle}}})
    execute_batch(reqs, concurrency=100)

def list_tablet_cells(data):
    reqs = []
    data = data.items()
    for bundle, count in data:
        reqs.append({"command": "get", "parameters": {
            "max_size": count,
            "path": "//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle)}})
    rsps = execute_batch(reqs, concurrency=100)
    cells = []
    for i in xrange(len(rsps)):
        cells += rsps[i]["output"][0:data[i][1]]
    return cells

def remove_tablet_cells(cells):
    reqs = []
    for cell in cells:
        reqs.append({"command": "remove", "parameters": {
            "path": "//sys/tablet_cells/{0}".format(cell)}})
    execute_batch(reqs, concurrency=100)


def show(args):
    yson.dump(get_tablet_cell_count_per_bundle(), sys.stdout, yson_format="pretty")

def save(args):
    if args.file is None:
        raise Exception("Need to specify file")
    data = get_tablet_cell_count_per_bundle()
    with open(args.file, "w") as f:
        yson.dump(data, f, yson_format="pretty")

def restore(args):
    if args.file is None:
        raise Exception("Need to specify file")
    with open(args.file, "r") as f:
        data = yson.load(f)
    create_tablet_cells(data)

def remove(args):
    if args.bundle is not None and args.config is not None:
        raise Exception("Cannot determine which tablet cells to remove: both bundle and config arguments are present")
    elif args.bundle:
        cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(args.bundle))
    elif args.config:
        cells = list_tablet_cells(yson.loads(args.config))
    else:
        cells = yt.get("//sys/tablet_cells")
    remove_tablet_cells(cells)

def create(args):
    if args.config is None:
        raise Exception("Need to specify config")
    create_tablet_cells(yson.loads(args.config))

def main2():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("show", "save", "restore", "remove", "create"))
    parser.add_argument("--config", type=str, default=None,
                        help="YSON-serialized map bundle -> tablet_cell_count")
    parser.add_argument("--bundle", type=str, default=None,
                        help="Tablet cell bundle name")
    parser.add_argument("--file", "--f", type=str, default=None,
                        help="File to save/restore config")
    args = parser.parse_args()

    if args.action == "show":
        show(args)
    elif args.action == "save":
        save(args)
    elif args.action == "restore":
        restore(args)
    elif args.action == "remove":
        remove(args)
    elif args.action == "create":
        create(args)

if __name__ == "__main__":
    main2()
