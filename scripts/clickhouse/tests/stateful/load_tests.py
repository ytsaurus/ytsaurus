#!/usr/bin/python2.7
import argparse
import os
import yt.wrapper as yt_wrapper


test_table_schema = [
        {'name': 'name', 'type': 'string', 'required': 'true'},
        {'name': 'queries', 'type_v3': { 'type_name': 'list', 'item': 'string' }, 'required': 'true'},
        {'name': 'reference', 'type': 'string', 'required': 'true'},
]


def extract_test_name_from_filename(filename):
    return '_'.join(filename.split('.')[0].split('_')[1:])


def replace_path_with_alias(query):
    query = query.replace("remote('127.0.0.{1,2}', test, hits)", "{hits}")
    query = query.replace("remote('127.0.0.{1,2}', test, visits)", "{visits}")

    query = query.replace("remote('127.0.0.{1,2}:9000', test, hits)", "{hits}")
    query = query.replace("remote('127.0.0.{1,2}:9000', test, visits)", "{visits}")

    query = query.replace("remote('localhost', test, hits)", "{hits}")
    query = query.replace("remote('localhost', test, visits)", "{visits}")

    query = query.replace("remote('127.0.0.{1,2}', test.hits)", "{hits}")
    query = query.replace("remote('127.0.0.{1,2}', test.visits)", "{visits}")

    query = query.replace("remote('127.0.0.{1,2,3,4,5,6,7,8,9,10}', test.hits)", "{hits}")
    query = query.replace("remote('127.0.0.{1,2,3,4,5,6,7,8,9,10}', test.visits)", "{visits}")

    query = query.replace("test.hits", "{hits}")
    query = query.replace("test.visits", "{visits}")

    return query


def load_tests(directory):
    tests = {}
    for filename in os.listdir(directory):
        test_name = filename.split('.')[0]
        if test_name not in tests:
            tests[test_name] = {'queries': [], 'reference': ''}
        if filename.endswith('.sql'):
            tests[test_name]['queries'] = [replace_path_with_alias(query) for query in open(directory + '/' + filename).read().rstrip().split(';') if query]
        elif filename.endswith('.reference'):
            tests[test_name]['reference'] = open(directory + '/' + filename).read().rstrip()
    return tests


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--output', help='Cypress output table path', required=True)
    parser.add_argument('input', help='Directory with sql queries and references')

    args=parser.parse_args()

    tests = load_tests(args.input)

    table = [{'name': name, 'queries': test['queries'], 'reference': test['reference']} for name, test in tests.items()]
    table.sort(key=lambda test: test['name'])

    yt_wrapper.create('table', args.output, attributes={'schema': test_table_schema})
    yt_wrapper.write_table(args.output, table)
