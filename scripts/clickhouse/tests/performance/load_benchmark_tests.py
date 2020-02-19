#!/usr/bin/python2.7
import argparse
import os
import yt
from yt import wrapper

test_table_schema = [
        {'name': 'name', 'type': 'string', 'required': 'true'},
        {'name': 'queries', 'type_v3': { 'type_name': 'list', 'item': 'string' }, 'required': 'true'},
        {'name': 'reference_execution_times', 'type_v3': { 'type_name': 'list', 'item': 'double' }, 'required': 'true'},
]


def replace_path_with_alias(query):
    query = query.replace("{table}", "{hits}")
    return query


def load_tests(path):
    queries = []
    with open(path) as file:
        for line in file:
            line = line.strip()
            queries.append(replace_path_with_alias(line))
    return queries


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--output', help='Cypress output table path', required=True)
    parser.add_argument('input', help='File with benchmark perfomance tests')

    args=parser.parse_args()

    tests = load_tests(args.input)

    print tests
    table = [{'name': 'benchmark', 'queries': [query for query in tests], 'reference_execution_times': [0. for query in tests]}]

    yt.wrapper.create('table', args.output, attributes={'schema': test_table_schema})
    yt.wrapper.write_table(args.output, table)
