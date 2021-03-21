#!/usr/bin/python2.7 -u
import argparse
from prettytable import PrettyTable
from yt import wrapper as yt_wrapper


def load_results(results_path):
    return list(yt_wrapper.read_table(yt_wrapper.TablePath(results_path)))


def print_comparison_report(target_table, reference_table):
    reference_execution_times = {}
    for reference_row in reference_table:
        query = reference_row['query']
        execution_time = reference_row['execution_time']
        reference_execution_times[query] = execution_time

    report = PrettyTable()
    report.field_names = ['', 'Reference', 'Target', 'Ratio', 'Percent Diff', 'Trace ID']
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
    print(report)


def get_operation_by_path(path):
    return yt_wrapper.get(path + "/@key")


def print_clique_configurations(target_operation, reference_operation):
    reference_operation_info = yt_wrapper.get_operation(reference_operation, attributes=["operation_type", "spec"])
    target_operation_info = yt_wrapper.get_operation(target_operation, attributes=["operation_type", "spec"])

    configuration_report = PrettyTable(max_width=0)
    configuration_report.field_names = ['', 'cpu_limit', 'job_count', 'operation_url']
    configuration_report.add_row([
        'Reference',
        reference_operation_info['spec']['tasks']['instances']['cpu_limit'],
        reference_operation_info['spec']['tasks']['instances']['job_count'],
        yt_wrapper.operation_commands.get_operation_url(reference_operation)])

    configuration_report.add_row([
        'Target',
        target_operation_info['spec']['tasks']['instances']['cpu_limit'],
        target_operation_info['spec']['tasks']['instances']['job_count'],
        yt_wrapper.operation_commands.get_operation_url(target_operation)])

    print(configuration_report)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--target', help='Cypress target test table path', required=True)
    parser.add_argument('--reference', help='Cypress reference test table path', required=False, default='//sys/clickhouse/tests/performance/reference')

    args = parser.parse_args()

    print_comparison_report(load_results(args.target), load_results(args.reference))
    print_clique_configurations(get_operation_by_path(args.target), get_operation_by_path(args.reference))
