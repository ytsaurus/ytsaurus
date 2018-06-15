#!/usr/bin/env python

import yt.wrapper as yt
import yt.yson as yson

import argparse
import sys
import os

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

def get_tablet_cell_count_per_bundle(args):
    bundles = [args.bundle] if args.bundle else yt.list("//sys/tablet_cell_bundles")
    reqs = []
    for bundle in bundles:
        reqs.append({"command": "get", "parameters": {
            "path": "//sys/tablet_cell_bundles/{0}/@tablet_cell_count".format(bundle)}})
    rsps = execute_batch(reqs, concurrency=100)
    config = {}
    for i in range(len(rsps)):
        config[bundles[i]] = rsps[i]["output"]
    return config

def create_tablet_cells(args, config):
    reqs = []
    for bundle, count in config.iteritems():
        if args.bundle and bundle != args.bundle:
            continue
        for i in xrange(count):
            reqs.append({"command": "create", "parameters": {
                "type": "tablet_cell", "attributes": {"tablet_cell_bundle": bundle}}})
    execute_batch(reqs, concurrency=100)

def list_empty_tablet_cells(args, config):
    reqs = []
    if args.bundle:
        config = {args.bundle: config.get(args.bundle, 0)}
    data = config.items()
    for bundle, count in data:
        reqs.append({"command": "get", "parameters": {
            "max_size": count,
            "path": "//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(bundle)}})
    rsps = execute_batch(reqs, concurrency=100)
    reqs = []
    bundles = []
    cells = []
    for i in xrange(len(rsps)):
        for cell in rsps[i]["output"]:
            reqs.append({"command": "get", "parameters": {
                "path": "//sys/tablet_cells/{0}/@tablet_count".format(cell)}})
            bundles.append(data[i][0])
            cells.append(cell)
    rsps = execute_batch(reqs, concurrency=100)
    count = {bundle:0 for bundle in config.keys()}
    response = []
    for i in xrange(len(rsps)):
        bundle = bundles[i]
        if rsps[i]["output"] == 0 and count[bundle] < config[bundle]:
            response.append(cells[i])
            count[bundle] += 1
    return count, response

def remove_tablet_cells(cells):
    reqs = []
    for cell in cells:
        reqs.append({"command": "remove", "parameters": {
            "path": "//sys/tablet_cells/{0}".format(cell)}})
    execute_batch(reqs, concurrency=100)


def show(args):
    yson.dump(get_tablet_cell_count_per_bundle(args), sys.stdout, yson_format="pretty")

def save(args):
    if args.file is None:
        raise Exception("Need to specify file")
    config = get_tablet_cell_count_per_bundle(args)
    if os.path.isfile(args.file) and not args.force:
        raise Exception("File \"{0}\" already exists. Use --force to overwrite it.".format(args.file))
    with open(args.file, "w") as f:
        yson.dump(config, f, yson_format="pretty")

def restore(args):
    if args.file is None:
        raise Exception("Need to specify file")
    with open(args.file, "r") as f:
        config = yson.load(f)
    create_tablet_cells(args, config)

def remove(args):
    if args.config:
        config, cells = list_empty_tablet_cells(args, yson.loads(args.config))
        print "Remove: ", config
    elif args.bundle:
        cells = yt.get("//sys/tablet_cell_bundles/{0}/@tablet_cell_ids".format(args.bundle))
    else:
        cells = yt.get("//sys/tablet_cells")
    remove_tablet_cells(cells)

def create(args):
    if args.config is None:
        raise Exception("Need to specify config")
    create_tablet_cells(args, yson.loads(args.config))

def main2():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("show", "save", "restore", "remove", "create"))
    parser.add_argument("--config", type=str, default=None,
                        help="YSON-serialized map bundle -> tablet_cell_count")
    parser.add_argument("--bundle", type=str, default=None,
                        help="Tablet cell bundle name")
    parser.add_argument("--file", "--f", type=str, default=None,
                        help="File to save/restore config")
    parser.add_argument("--force", action="store_true", default=None,
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
