#!/usr/bin/python3

import argparse

from start_clique import start_clique
from tester import invoke_testset, compare_performance

import datetime
import logging


logger = logging.getLogger(__name__)


def setup_logging(args):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s\t%(levelname).1s\t%(module)s:%(lineno)d\t%(message)s")
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel([logging.INFO, logging.DEBUG][min(args.verbose, 1)])
    stderr_handler.setFormatter(formatter)
    file_handler = logging.FileHandler("tester.debug.log", "a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(stderr_handler)
    root_logger.addHandler(file_handler)
    root_logger.info("Logging started, invocation args = %s", args)


def accept_on_setup(testset_key, alias, annotation, attributes, compare_with_baseline, dry_run=False):
    invocation = invoke_testset(testset_key, alias, dry_run=dry_run, annotation=annotation, attributes=attributes,
                                tag_expression="not xfail and not skip")
    if compare_with_baseline:
        compare_performance(invocation.invocation_key, testset_key=testset_key, dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", help="Arbitrary lowercase_with_underscore tag for clique name")
    parser.add_argument("--bin-path", help="Cypress binary path")
    parser.add_argument("--dry-run", help="Print what is to be done, do not actually run anything",
                        action="store_true")
    parser.add_argument("--clickhouse-config", help="Config patch")
    parser.add_argument("--spec", help="Spec patch in YSON")
    parser.add_argument('-v', '--verbose', action='count', default=0, help="Log stuff, more v's for more logging")

    args = parser.parse_args()

    setup_logging(args)

    kind_to_alias = dict()

    args_dict = dict(vars(args))
    if "verbose" in args_dict:
        del args_dict["verbose"]

    for kind in ("correctness1", "correctness2", "performance"):
        kind_to_alias[kind] = start_clique(**args_dict, kind=kind)

    setups = [
        {
            "kind": "correctness1",
            "testset_key": "correctness",
        },
        {
            "kind": "correctness2",
            "testset_key": "correctness",
        },
        {
            "kind": "performance",
            "testset_key": "performance",
            "compare_with_baseline": True,
        },
    ]

    now = datetime.datetime.now().isoformat()

    for setup in setups:
        annotation = "Acceptance invocation for clique kind {} on testset {} on {}".format(
            setup["kind"], setup["testset_key"], now)
        attributes = {
            "clique_kind": setup["kind"],
            "clique_tag": args.tag,
        }
        accept_on_setup(setup["testset_key"], kind_to_alias[setup["kind"]], annotation, attributes,
                        setup.get("compare_with_baseline", False), dry_run=args.dry_run)


if __name__ == "__main__":
    main()
