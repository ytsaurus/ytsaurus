#!/usr/bin/python2.7
import argparse
import os
import yt.wrapper as yt_wrapper
import logging
from tqdm import tqdm
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s\t%(levelname).1s\t%(module)s:%(lineno)d\t%(message)s')


unsupported_tests = [
        '00010_quantiles_segfault', # uses remote, same as local clickhouse
        '00090_thread_pool_deadlock', # test of clickhouse-client
#       '00024_random_counters', # too big
        '00031_array_enumerate_uniq',
        '00067_union_all', # uses remote, same as local clickhouse
        '00147_global_in_aggregate_function', # uses remote, same as local clickhouse
        '00149_quantiles_timing_distributed', # uses distribution
]

unsupported_query_words = ['ParsedParams', 'GeneralInterests', 'SET', 'CREATE', 'URLCategories', 'GoalsReached', 'Goals', 'PREWHERE', 'DROP', 'RENAME', 'system.columns']

def is_query_supported(query):
    for unsupported_word in unsupported_query_words:
        if unsupported_word.lower() in query.lower():
            return False
    return True


def load_tests(tests_path):
    return list(yt_wrapper.read_table(yt_wrapper.TablePath(tests_path)))


class QueryExecutor:
    def __init__(self, hits_path, visits_path, cluster, clique_id):
        self.hits_path = hits_path
        self.visits_path = visits_path
        self.cluster = cluster
        self.clique_id = clique_id
        self.timeout = 600

    def patch_paths(self, query):
        return query.replace("{hits}", '"' + self.hits_path + '"').replace("{visits}", '"' + self.visits_path + '"')

    def execute(self, query):
        query = self.patch_paths(query)
        session = requests.Session()
        url = "http://{cluster}.yt.yandex.net/query?database={clique_id}&password={token}".format(
                cluster=self.cluster, clique_id=self.clique_id, token=yt_wrapper._get_token())
        responce = session.post(url, data=query, timeout=self.timeout)
        if responce.status_code != 200:
            logging.error("error while executing query: status_code: {status_code} headers: {headers} content: {content}".format(
                    status_code=responce.status_code, headers=responce.headers, content=responce.content))
            raise ValueError
        responce.raise_for_status()
        return responce.content.rstrip().split('\n')


def execute_tests(tests, query_executor):
    success_count = 0
    fail_count = 0
    skip_count = 0

    for test in tests:
        print "executing test:", test['name'], "...",
        skipped = test['name'] in unsupported_tests
        reference = test['reference'].split('\n')
        result = []
        if not skipped:
            for query in test['queries']:
                if not is_query_supported(query):
                    skipped = True
                    break
                try:
                    responce = query_executor.execute(query)
                except ValueError:
                    print "EXCEPTION"
                    logging.critical("raised exception on query: {}".format(query_executor.patch_paths(query)))
                    exit()
                if responce != [''] or len(test['queries']) == 1:
                    result.extend(responce)

        if skipped:
            print "SKIPPED"
            skip_count += 1
        elif result == reference:
            print "OK"
            success_count += 1
        else:
            print "FAILED"
            logging.error("{} failed on queries: {}".format(test['name'], test['queries']))
            logging.error("result:    {}".format(result))
            logging.error("reference: {}".format(reference))
            fail_count += 1

    print "total tests:", len(tests), "success_count:", success_count, "fail_count:", fail_count, "skip_count:", skip_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--tests', help='Cypress test table path', required=True)
    parser.add_argument('--hits', help='Cypress hits table path', required=True)
    parser.add_argument('--visits', help='Cypress visits table path', required=True)
    parser.add_argument('--cluster', help='Cluster name', required=True)
    parser.add_argument('--clique_id', help='ClickHouse clique id', required=True)

    args=parser.parse_args()

    execute_tests(load_tests(args.tests), QueryExecutor(args.hits, args.visits, args.cluster, args.clique_id))

