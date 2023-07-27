#!/usr/bin/env python

import os
import sys
from os.path import isdir, join, exists, basename, dirname

import codecs
import ujson as json

from collections import OrderedDict
import re

number_pattern = r"\d+(,\d+)*"
float_number_pattern = r"\d+(.\d+)?"

preempted_jobs_pattern = re.compile("preempted jobs: ({})".format(number_pattern))
total_jobs_pattern = re.compile("Total jobs: ({})".format(number_pattern))
preempted_duration_pattern = re.compile("preempted duration: ({}s)".format(float_number_pattern))
total_jobs_duration_pattern = re.compile("jobs duration: ({}s)".format(float_number_pattern))

underserved_penalty_pattern = re.compile("underserved penalty: ({})".format(float_number_pattern))
underserved_duration_pattern = re.compile("underserved duration: ({})".format(float_number_pattern))

underserved_fraction_pattern = re.compile("tolerance: {}, underserved_fraction: ({}/{})"
                                        .format(float_number_pattern, number_pattern, number_pattern))

url_base = "http://build01-01g.yt.yandex.net/pages/"
start_path = "."

schema = OrderedDict.fromkeys([
    "Name", "Jobs", "Preempted jobs", "Preempted duration", "Total jobs duration",
    "Underserved penalty (pools)", "Underserved duration (pools)",
    "Underserved penalty (operations)", "Underserved duration (operations)",
    "Underserved penalty (sum)", "Underserved duration (sum)",
], "NULL")

reports = [
    ("Analyze_simulation.html", "Analysis"),
    ("Measure_fairness_pools.html", "Fairness_pools"),
    ("Measure_fairness_operations.html", "Fairness_operations"),
]

def remove_start_path(path):
    return path[len(start_path):]

def get_metainfo(path):
    metainfo = {}
    config_path = join(path, "config.json")
    metainfo["name"] = basename(path)
    metainfo["description"] = ""

    if exists(config_path):
        metainfo.update(json.loads(open(config_path, "r").read()))

    return metainfo

def get_links(path):
    links = []

    for filename, name in reports:
        file_path = join(path, filename)
        if exists(file_path):
            links.append({"name": name, "path": file_path})

    return links

def build_tree(path):
    tree = {}
    tree["children"] = []
    tree["path"] = path
    tree.update(get_metainfo(path))
    tree["links"] = get_links(path)
    for entry in os.listdir(path):
        if entry == "partition_sort_investigation":
            continue
        child_path = join(path, entry)
        if isdir(child_path) and entry != 'data':
            tree["children"].append(build_tree(child_path))
    tree["children"] = sorted(tree["children"], key=lambda child: child["name"])
    return tree

def indent(depth):
    return "  " * (depth > 0)

def generate_links_page(tree, depth):
    page = ""

    if depth > 0:
        page += indent(depth)
        page += "\n* {[ " + "**" + tree["name"] + "**"
        if tree["description"]:
            page += "\n" + tree["description"]

        if tree["links"]:
            page += "\n"
            for link in tree["links"]:
                url = url_base + remove_start_path(link["path"])
                page += "(({} {})), ".format(url, link["name"])

    for child in tree["children"]:
        page += generate_links_page(child, depth + 1)

    if depth > 0:
        page += indent(depth)
        page += "]}"

    return page

def group_or_zero(search_result, k=1):
    if not search_result:
        return 0
    else:
        return search_result.group(k)

def extract_simulation_info(path):
    result = {}
    content = open(path, "r").read()
    result["Preempted jobs"] = group_or_zero(preempted_jobs_pattern.search(content))
    result["Jobs"] = group_or_zero(total_jobs_pattern.search(content))
    result["Preempted duration"] = group_or_zero(preempted_duration_pattern.search(content))
    result["Total jobs duration"] = group_or_zero(total_jobs_duration_pattern.search(content))
    return result

def extract_fairness_info(path):
    result = {}
    content = open(path, "r").read()
    if path.endswith("pools.html"):
        target = "pools"
    else:
        target = "operations"

    result["Underserved penalty (" + target + ")"] = group_or_zero(underserved_penalty_pattern.search(content))
    result["Underserved duration (" + target + ")"] = group_or_zero(underserved_duration_pattern.search(content))
    return result

def extract_report_info(path):
    filename = basename(path)
    if filename == reports[0][0]:
        return extract_simulation_info(path)
    else:
        return extract_fairness_info(path)

def update_sum(row):
    result = {}
    total_penalty = 0
    total_duration = 0
    for target in ["pools", "operations"]:
        total_penalty += float(row["Underserved penalty (" + target + ")"])
        total_duration += float(row["Underserved duration (" + target + ")"])
    result["Underserved penalty (sum)"] = total_penalty
    result["Underserved duration (sum)"] = total_duration
    row.update(result)

def generate_table_row(values):
    row = "||"
    #print(values)
    for i in xrange(len(values)):
        if i > 0:
            row += "|"
        row += str(values[i])
    row += "||"
    return row

def generate_table_page(tree, depth, pretty_name):
    page = ""

    if "type" in tree and tree["type"] == "series":
        page += "**" + tree["name"] + "**" + "\n"
        page += tree["description"] + "\n"
        page += "#|\n"
        page += generate_table_row(schema.keys()) + "\n"

    if depth > 1:
        pretty_name += tree["name"] + "/"

    if depth > 0:
        row = schema.copy()
        for link in tree["links"]:
            row.update(extract_report_info(link["path"]))
        if tree["links"]:
            row["Name"] = pretty_name[:-1]
            update_sum(row)
            page += generate_table_row(row.values()) + "\n"

    for child in tree["children"]:
        page += generate_table_page(child, depth + 1, pretty_name)

    if "type" in tree and tree["type"] == "series":
        page += "|#\n"

    return page

def generate_wikipage(tree):
    page = ""
    page += generate_links_page(tree, 0)
    page += "\n\n\n"
    page += generate_table_page(tree, 0, "")
    return page

def main():
    if len(sys.argv) != 2:
        print("Usage: {} PATH".format(sys.argv[0]))
    global start_path
    start_path = sys.argv[1]
    tree = build_tree(start_path)
    UTF8Writer = codecs.getwriter('utf8')
    sys.stdout = UTF8Writer(sys.stdout)
    print(generate_wikipage(tree))

if __name__ == "__main__":
    main()
