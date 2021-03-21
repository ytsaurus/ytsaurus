#!/usr/bin/python2.7 -u
import argparse
import logging
import requests
import time
import yt
import yt.clickhouse as chyt
from yt.wrapper.http_helpers import get_token
from yt.wrapper.http_driver import HeavyProxyProvider
from flush_results import flush_results
from print_comparison_report import print_comparison_report
from print_comparison_report import print_clique_configurations
from print_comparison_report import get_operation_by_path

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
        session.headers['Authorization'] = 'OAuth {token}'.format(token=get_token())
        url = "http://{proxy}/query?database={clique_id}".format(proxy=HeavyProxyProvider(None)(), clique_id=self.clique_id)

        start_time = time.time()
        response = session.post(url, data=query, timeout=self.timeout, headers=session.headers)
        if response.status_code != 200:
            logging.error(
                "error while executing query: status_code: {status_code} headers: {headers} content: {content}".format(
                    status_code=response.status_code, headers=response.headers, content=response.content))
            raise ValueError
        response.raise_for_status()
        finish_time = time.time()

        return finish_time - start_time, response.headers['X-YT-Trace-Id'].split(', ')[0]


def execute_queries(queries, query_repetitions, query_executor):
    execution_times = []
    trace_ids = []
    print 'executing', len(queries), 'queries:'
    for query_index in range(len(queries)):
        query = queries[query_index]
        if not is_query_supported(query):
            execution_times.append(0.)
            trace_ids.append('---')
            continue
        try:
            query_execution_times_and_trace_ids = []
            for i in range(query_repetitions):
                if query_repetitions > 1:
                    print str(query_index) + '.' + str(i),
                execution_time, trace_id = query_executor.measure_execution_time(query)
                if query_repetitions > 1:
                    print 'execution_time: %.2f' % round(execution_time, 2) + 's trace_id:', trace_id
                query_execution_times_and_trace_ids.append((execution_time, trace_id))
            min_execution_time, min_trace_id = min(query_execution_times_and_trace_ids)
            print query_index, 'finished\n  min_execution_time: %.2f' % min_execution_time + 's\n  min_trace_id: ', min_trace_id, '\n'
            execution_times.append(min_execution_time)
            trace_ids.append(min_trace_id)
        except ValueError:
            print 'EXCEPTION'
            logging.exception('raised exception on query: {}'.format(query_executor.patch_paths(query)))
            execution_times.append(0.)
            trace_ids.append('---')

    print ' done with total time:', sum(execution_times)
    return execution_times, trace_ids


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--tests', help='Cypress test table path', required=True)
    parser.add_argument('--hits', help='Cypress hits table path', required=True)
    parser.add_argument('--clique-id', help='ClickHouse clique id', required=True)
    parser.add_argument('--results-directory', help='Cypress results directory path', required=False, default='//sys/clickhouse/tests/performance/results')
    parser.add_argument('--query-repetitions', help='Query repetitions to get min time', required=False, default=1, type=int)

    args = parser.parse_args()

    resolve_alias_result = chyt.clickhouse._resolve_alias(args.clique_id)
    if not resolve_alias_result:
        print "unknown clique:", args.clique_id
        exit()

    operation_id = resolve_alias_result['id']
    print 'start perf tests on operation:', operation_id

    reference_tests = load_tests(args.tests)
    queries = [test['query'] for test in reference_tests]

    execution_times, trace_ids = execute_queries(queries, args.query_repetitions, QueryExecutor(args.hits, args.clique_id))

    result_tests = flush_results(args.results_directory, operation_id, queries, execution_times, trace_ids)
    print_comparison_report(result_tests, reference_tests)

    print_clique_configurations(get_operation_by_path(args.tests), operation_id)
