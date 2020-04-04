#!/usr/bin/python2.7 -u
import argparse
from texttable import Texttable
from yt import wrapper as yt_wrapper

def load_results(results_path):
    return list(yt_wrapper.read_table(yt_wrapper.TablePath(results_path)))


def print_comparison_report(target_table, reference_table):
    reference_execution_times = {}
    for reference_row in reference_table:
        query = reference_row['query']
        execution_time = reference_row['execution_time']
        reference_execution_times[query] = execution_time

    report = Texttable(max_width=0)
    report.add_row(['', 'Reference', 'Current', 'Ratio', 'Percent Diff', 'Trace ID'])
    for row_index in range(len(target_table)):
        target_row = target_table[row_index]
        query = target_row['query']
        execution_time = target_row['execution_time']
        trace_id = target_row['trace_id']

        reference_execution_time = None
        if query in reference_execution_times:
            reference_execution_time = reference_execution_times[query]

        report_row = [row_index, reference_execution_time, execution_time]
        if reference_execution_time:
            percent_diff = round((execution_time - reference_execution_time) / reference_execution_time * 100, 2)
            report_row.extend([execution_time / reference_execution_time, str(percent_diff) + '%'])
        else:
            report_row.extend(['---', '---'])
        report_row.extend([trace_id])
        report.add_row(report_row)
    print(report.draw())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--target', help='Cypress target test table path', required=True)
    parser.add_argument('--reference', help='Cypress reference test table path', required=False, default='//sys/clickhouse/tests/performance/reference')

    args=parser.parse_args()

    print_comparison_report(load_results(args.target), load_results(args.reference))
