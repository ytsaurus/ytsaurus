#!/usr/bin/python2.7 -u
import argparse
import logging
import os
import requests
import time
import yt
from texttable import Texttable
from tqdm import tqdm
from yt import wrapper

logging.basicConfig(level=logging.INFO, format='%(asctime)s\t%(levelname).1s\t%(module)s:%(lineno)d\t%(message)s')


unsupported_tests = []

unsupported_query_words = ['OriginalURL', 'arrayFill', 'system.numbers', 'system.{table}']

def is_query_supported(query):
    for unsupported_word in unsupported_query_words:
        if unsupported_word.lower() in query.lower():
            return False
    return True


def load_tests(tests_path):
    return list(yt.wrapper.read_table(yt.wrapper.TablePath(tests_path)))


class QueryExecutor:
    def __init__(self, hits_path, cluster, clique_id):
        self.hits_path = hits_path
        self.cluster = cluster
        self.clique_id = clique_id
        self.timeout = 600

    def patch_paths(self, query):
        return query.replace('{hits}', '"' + self.hits_path + '"')

    def measure_execution_time(self, query):
        query = self.patch_paths(query)
        session = requests.Session()
        url = 'http://{cluster}.yt.yandex.net/query?database={clique_id}&password={token}'.format(
                cluster=self.cluster, clique_id=self.clique_id, token=yt.wrapper._get_token())
        start_time = time.time()
        response = session.post(url, data=query, timeout=self.timeout)
        if response.status_code != 200:
            logging.error('error while executing query: status_code: {status_code} headers: {headers} content: {content}'.format(
                    status_code=response.status_code, headers=response.headers, content=response.content))
            raise ValueError
        response.raise_for_status()
        finish_time = time.time()
        return finish_time - start_time, response.headers['X-YT-Trace-Id']


def execute_tests(tests, query_executor):
    execution_times = {}
    trace_ids = {}
    for test in tests:
        skipped = test['name'] in unsupported_tests
        if not skipped:
            print 'executing test "' + test['name'] + '" with', len(test['queries']), 'queries:',
            test_execution_times = []
            test_trace_ids = []
            for query_index in range(len(test['queries'])):
                print query_index,
                query = test['queries'][query_index]
                if not is_query_supported(query):
                    skipped = True
                    test_execution_times.append(-1)
                    test_trace_ids.append(-1)
                    break
                try:
                    execution_time, trace_id = query_executor.measure_execution_time(query)
                    test_execution_times.append(execution_time)
                    test_trace_ids.append(trace_id)
                except ValueError:
                    print 'EXCEPTION'
                    logging.critical('raised exception on query: {}'.format(query_executor.patch_paths(query)))
                    exit()
            execution_times[test['name']] = test_execution_times
            trace_ids[test['name']] = test_trace_ids
            print ' done with total time:', sum(test_execution_times)
    return execution_times, trace_ids


def print_report(tests, execution_times, trace_ids):
    for test in tests:
        test_name = test['name']
        print 'report for test:', test_name
        test_report = Texttable(max_width=0)
        test_report.add_row(['', 'Reference', 'Current', 'Ratio', 'Percent Diff', 'Trace ID'])
        for query_index in range(len(test['queries'])):
            query = test['queries'][query_index]
            reference_execution_time = test['reference_execution_times'][query_index]
            current_execution_time = execution_times[test['name']][query_index]
            trace_id = trace_ids[test['name']][query_index]
            report_row = [query_index, reference_execution_time, current_execution_time]
            if reference_execution_time != 0:
                percent_diff = round((current_execution_time - reference_execution_time) / reference_execution_time * 100, 2)
                report_row.extend([current_execution_time / reference_execution_time, str(percent_diff) + '%'])
            else:
                report_row.extend(['---', '---'])
            report_row.extend([trace_id])
            test_report.add_row(report_row)
        print(test_report.draw())

def override_reference(tests_path, tests, execution_times):
    test_table_schema = [
            {'name': 'name', 'type': 'string', 'required': 'true'},
            {'name': 'queries', 'type_v3': { 'type_name': 'list', 'item': 'string' }, 'required': 'true'},
            {'name': 'reference_execution_times', 'type_v3': { 'type_name': 'list', 'item': 'double' }, 'required': 'true'},
    ]
    test_table = [{'name': test['name'], 'queries': test['queries'], 'reference_execution_times': execution_times[test['name']]} for test in tests]
    yt.wrapper.write_table(tests_path, test_table)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--tests', help='Cypress test table path', required=True)
    parser.add_argument('--hits', help='Cypress hits table path', required=True)
    parser.add_argument('--cluster', help='Cluster name', required=True)
    parser.add_argument('--clique-id', help='ClickHouse clique id', required=True)
    parser.add_argument('--override-reference', help='Override reference exection time', action='store_true')

    args=parser.parse_args()

    tests = load_tests(args.tests)
    execution_times, trace_ids = execute_tests(tests, QueryExecutor(args.hits, args.cluster, args.clique_id))

    print_report(tests, execution_times, trace_ids)

    if args.override_reference:
        override_reference(args.tests, tests, execution_times)
