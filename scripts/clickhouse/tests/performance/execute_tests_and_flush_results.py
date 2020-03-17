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
from flush_results import flush_results
from print_comparison_report import print_comparison_report

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
    def __init__(self, hits_path, clique_id):
        self.hits_path = hits_path
        self.clique_id = clique_id
        self.timeout = 600

    def patch_paths(self, query):
        return query.replace('{hits}', '"' + self.hits_path + '"')

    def measure_execution_time(self, query):
        query = self.patch_paths(query)
        session = requests.Session()
        url = 'http://{proxy_url}/query?database={clique_id}&password={token}'.format(
                proxy_url=wrapper._get_proxy_url(), clique_id=self.clique_id, token=yt.wrapper._get_token())
        start_time = time.time()
        response = session.post(url, data=query, timeout=self.timeout)
        if response.status_code != 200:
            logging.error('error while executing query: status_code: {status_code} headers: {headers} content: {content}'.format(
                    status_code=response.status_code, headers=response.headers, content=response.content))
            raise ValueError
        response.raise_for_status()
        finish_time = time.time()
        return finish_time - start_time, response.headers['X-YT-Trace-Id']


def execute_queries(queries, query_repetitions, query_executor):
    execution_times = []
    trace_ids = []
    print 'executing', len(queries), 'queries:',
    for query_index in range(len(queries)):
        query = queries[query_index]
        if not is_query_supported(query):
            skipped = True
            execution_times.append(None)
            trace_ids.append(None)
            break
        try:
            query_execution_times_and_trace_ids = []
            for i in range(query_repetitions):
                if query_repetitions > 1:
                    print str(query_index) + '.' + str(i),
                else:
                    print query_index,
                execution_time, trace_id = query_executor.measure_execution_time(query)
                query_execution_times_and_trace_ids.append((execution_time, trace_id))
            min_execution_time, min_trace_id = min(query_execution_times_and_trace_ids)
            execution_times.append(min_execution_time)
            trace_ids.append(min_trace_id)
        except ValueError:
            print 'EXCEPTION'
            logging.critical('raised exception on query: {}'.format(query_executor.patch_paths(query)))
            exit()
    print ' done with total time:', sum(execution_times)
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--tests', help='Cypress test table path', required=True)
    parser.add_argument('--hits', help='Cypress hits table path', required=True)
    parser.add_argument('--clique-id', help='ClickHouse clique id', required=True)
    parser.add_argument('--results-directory', help='Cypress results directory path', required=False, default='//sys/clickhouse/tests/performance/results')
    parser.add_argument('--query-repetitions', help='Query repetitions to get min time', required=False, default=1, type=int)

    args=parser.parse_args()

    operation_id = yt.wrapper.clickhouse._resolve_alias(args.clique_id)['id']
    print 'start perf tests on operation:', operation_id

    reference_tests = load_tests(args.tests)
    queries = [test['query'] for test in reference_tests]

    execution_times, trace_ids = execute_queries(queries, args.query_repetitions, QueryExecutor(args.hits, args.clique_id))

    result_tests = flush_results(args.results_directory, operation_id, queries, execution_times, trace_ids)
    print_comparison_report(result_tests, reference_tests)
