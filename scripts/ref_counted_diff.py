#!/usr/bin/env python

from __future__ import print_function

import yt.yson as yson

import argparse
import itertools
import json


def get_statistics(result):
    if "ref_counted" in result:
        result = result["ref_counted"]
    return result["statistics"]


def load(filename, format_):
    file_data = open(filename).read()
    if format_ == "yson":
        raw_results = yson.loads(file_data) 
    elif format_ == "json":
        raw_results = json.loads(file_data) 
    else:
        raise NotImplementedError("Unknown format '{}'".format(format_))
    result = {}
    for obj in get_statistics(raw_results):
        result[obj["name"]] = obj
    return result


def format_value(value, field_name):
    if field_name.startswith("bytes"):
        return "{} MB".format(round(value / 1024.0 / 1024.0, 3))
    return str(value)


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
    field_names = ("objects_alive", "bytes_alive", "objects_allocated", "bytes_allocated")
    for name, count in sorted(diff.items(), key=lambda item: -item[1]):
        if count == 0:
            continue
        print(name)
        for field_name in field_names:
            print("    {}: {}".format(
                field_name,
                format_value(get_diff(stat1, stat2, name, field_name), field_name),
            ))
    print("\nTotal diff:")
    for field_name in field_names:
        print(
            field_name,
            format_value(
                sum(map(lambda name: get_diff(stat1, stat2, name, field_name), names)),
                field_name,
            )
        )


def parse_arguments():
    parser = argparse.ArgumentParser("Calculate difference between ref counted tracker statistics")
    parser.add_argument("first_file_path", type=str, default="ref_counted1")
    parser.add_argument("second_file_path", type=str, default="ref_counted2")
    parser.add_argument("--format", type=str, default="json")
    parser.add_argument("--sort-field-name", type=str, default="objects_alive")
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_arguments())
