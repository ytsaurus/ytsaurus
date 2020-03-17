#!/usr/bin/python2.7
import argparse
from flush_results import flush_results

def replace_path_with_alias(query):
    query = query.replace("{table}", "{hits}")
    return query


def load_queries(path):
    queries = []
    with open(path) as file:
        for line in file:
            line = line.strip()
            queries.append(replace_path_with_alias(line))
    return queries


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--results-directory', help='Cypress results directory', required=True)
    parser.add_argument('input', help='File with benchmark perfomance queries')

    args=parser.parse_args()

    queries = load_queries(args.input)

    flush_results(
            args.results_directory,
            "dry_tests",
            queries,
            [0.] * len(queries),
            [''] * len(queries)
    )
