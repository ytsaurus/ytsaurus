#!/usr/bin/python2.7
import argparse
from yt import wrapper as yt_wrapper

def flush_results(results_directory, operation_id, queries, execution_times, trace_ids):
    results_path = results_directory + '/' + operation_id
    results_table_schema = [
        {'name': 'query', 'type': 'string', 'required': 'true'},
        {'name': 'execution_time', 'type': 'double', 'required': 'true'},
        {'name': 'trace_id', 'type': 'string', 'required': 'true'},
    ]
    results_table = [{
            'query': queries[test_index],
            'execution_time' : execution_times[test_index],
            'trace_id': trace_ids[test_index]
        } for test_index in range(len(queries))]
    if not yt_wrapper.exists(results_path):
        yt_wrapper.create('table', results_path, attributes={'schema': results_table_schema})
    yt_wrapper.write_table(results_path, results_table)

    print 'results has been flushed to path:', results_path

    return results_table
