#!/usr/bin/env python

from __future__ import print_function

import yt.yson as yson

import argparse
import itertools
import json


def load(filename, format_):
    result = {}
    if format_ == "yson":
        for obj in yson.loads(open(filename).read())["statistics"]:
            result[obj["name"]] = obj
    elif format_ == "json":
        for obj in json.loads(open(filename).read())["statistics"]:
            result[obj["name"]] = obj
    else:
        raise NotImplementedError("Unknown format '{}'".format(format_))
    return result


def get_diff(stat1, stat2, name, field_name):
    return stat2.get(name, {}).get(field_name, 0) - stat1.get(name, {}).get(field_name, 0)


def main(arguments):
    stat1 = load(arguments.first_file_path, arguments.format)
    stat2 = load(arguments.second_file_path, arguments.format)
    diff = {}
    names = list(set(itertools.chain(stat1.iterkeys(), stat2.iterkeys())))
    for name in names:
        assert name not in diff
        diff[name] = get_diff(stat1, stat2, name, arguments.sort_field_name)
    for name, count in sorted(diff.items(), key=lambda item: -item[1]):
        if count == 0:
            continue
        print(
            name,
            get_diff(stat1, stat2, name, "objects_alive"),
            get_diff(stat1, stat2, name, "bytes_alive"),
        )
    print("Total diff:")
    for field_name in ("objects_alive", "bytes_alive"):
        print(field_name, sum(map(lambda name: get_diff(stat1, stat2, name, field_name), names)))


def parse_arguments():
    parser = argparse.ArgumentParser("Calculate difference between ref counted tracker statistics")
    parser.add_argument("first_file_path", type=str, default="ref_counted1")
    parser.add_argument("second_file_path", type=str, default="ref_counted2")
    parser.add_argument("--format", type=str, default="yson")
    parser.add_argument("--sort-field-name", type=str, default="objects_alive")
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_arguments())
