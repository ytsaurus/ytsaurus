#!/usr/bin/python3

import yt.wrapper as yt
import yt.yson as yson
import yt.clickhouse as chyt

import colorama

import copy
import re
import collections
import typing
import argparse
import logging
import prettytable
import random
import time
import math

logger = logging.getLogger(__name__)

CLIQUE_LOCK_PATH = "//sys/clickhouse/tests/clique_lock"
TESTSET_DIR_PATH = "//sys/clickhouse/tests/testsets"
INVOCATIONS_DIR_PATH = "//sys/clickhouse/tests/invocations"
BASELINE_INVOCATIONS_PATH = "//sys/clickhouse/tests/baseline_invocations"
# xfail means unexpectedly broken.
# skip means intentionally not working.
GLOBAL_TAGS = {"xfail", "skip"}


def to_names(schema):
    return [column["name"] for column in schema]


def to_namedtuple(name, schema):
    return collections.namedtuple(name, to_names(schema))


def node_snapshot(path):
    return yt.get(path + "/@", attributes=["id", "path", "revision", "content_revision",
                                           "creation_time", "modification_time"])


def to_struct(struct_type, row):
    # This is nasty.
    def maybe_decode_pair(key, value):
        key = key.decode("utf-8")
        if key != "reference" and isinstance(value, bytes):
            value = value.decode("utf-8")
        if key in ("tags", "queries"):
            value = [tag.decode("utf-8") for tag in value]
        return key, value

    row = dict(maybe_decode_pair(key, value) for key, value in row.items())
    return struct_type(**row)


def mutate_struct(struct, key, value):
    return type(struct)(**{k: v if k != key else value for k, v in struct._asdict().items()})


class Testset:
    SCHEMA = [
        {"name": "name", "type": "string"},
        {"name": "queries", "type_v3": {"type_name": "list", "item": "string"}},
        {"name": "reference", "type_v3": {"type_name": "optional", "item": "string"}},
        {"name": "tags", "type_v3": {"type_name": "list", "item": "string"}},
    ]
    TestSpec = to_namedtuple("TestSpec", SCHEMA)
    ATTRIBUTES = ["key", "path", "row_count", "annotation", "default_table_paths"]

    _tests: typing.Optional[typing.List[TestSpec]]

    def __init__(self, attributes):
        self.row_count = attributes.get("row_count", None)
        self.annotation = attributes.get("annotation", None)
        self.default_table_paths = attributes.get("default_table_paths", {})
        self.path = attributes["path"]
        self.key = attributes["key"]
        self._tests = None

    def fetch_tests(self):
        logger.debug("Fetching tests for testset %s", self.key)
        self._tests = [to_struct(Testset.TestSpec, row)
                       for row in yt.read_table(self.path, format=yt.YsonFormat(encoding=None))]

    def tests(self) -> typing.List[TestSpec]:
        if self._tests is None:
            raise yt.YtError("Testset {} was not fetched".format(self.path))
        return self._tests

    def write(self):
        logger.debug("Writing tests for testset %s", self.key)
        yt.write_table(self.path, [test_spec._asdict() for test_spec in self._tests])

    def tag_test(self, name, tags):
        index = None
        for i, test_spec in enumerate(self._tests):
            if test_spec.name == name:
                assert index is None
                index = i
        if index is None:
            raise yt.YtError("Test with name {} not found".format(name))
        self._tests[index] = mutate_struct(self._tests[index], "tags", tags)

    def known_tags(self):
        return set(tag for test_spec in self._tests for tag in test_spec.tags) | GLOBAL_TAGS


class Invocation:
    SCHEMA = [
        {"name": "name", "type": "string"},
        {"name": "run_index", "type": "int64"},
        # One of skipped, ok, ok_no_reference, fail, wrong_output.
        {"name": "outcome", "type": "string"},
        {"name": "queries", "type_v3": {"type_name": "list", "item": "string"}},
        {"name": "reference", "type_v3": {"type_name": "optional", "item": "string"}},
        # Filled only if reference is present.
        {"name": "output", "type_v3": {"type_name": "optional", "item": "string"}},
        {"name": "execution_time", "type_v3": {"type_name": "optional", "item": "double"}},
        {"name": "error", "type": "any"},
    ]
    TestResult = to_namedtuple("TestResult", SCHEMA)

    tx: typing.Optional[yt.Transaction]
    results: typing.List[TestResult]

    def __init__(self, testset: Testset, table_paths: typing.Dict[str, str], alias: str, operation_id: str):
        self.testset = testset
        self.table_paths = table_paths
        self.alias = alias
        self.operation_id = operation_id
        # Visually distinguish runs related to the same clique.
        self.invocation_key = None
        self.environment = None
        self.tx = None
        self.clique_version = None
        self.results = []
        self.invocation_path = None
        self.salt = None
        self.name_regex = None
        self.tag_expression = None
        self.run_count = None
        self.outcome_statistics = None

    def abort_tx(self):
        if self.tx is not None:
            logger.info("Aborting tx %s", self.tx.transaction_id)
            self.tx.abort()

    def prepare(self, dry_run=False) -> None:
        """
        Take exclusive locks on testset and test tables, shared lock on clique operation id,
        save environmnent information on tables, create invocation table.
        """
        self.tx = yt.Transaction()
        logger.debug("Working under tx %s", self.tx.transaction_id)

        if not dry_run:
            with yt.Transaction(transaction_id=self.tx.transaction_id):
                # We take exclusive lock on tables as a convenience matter: we do not want
                # several tests to read same tables as this may affect query running times.
                for path in [self.testset.path] + list(self.table_paths.values()):
                    logger.debug("Taking exclusive lock on %s", path)
                    yt.lock(path, "exclusive")
                logger.debug("Taking shared lock on %s for child %s", CLIQUE_LOCK_PATH, self.operation_id)
                yt.lock(CLIQUE_LOCK_PATH, "shared", child_key=self.operation_id)

        self.environment = {
            "testset": node_snapshot(self.testset.path),
            "tables": {table_key: node_snapshot(table_path) for table_key, table_path in self.table_paths.items()},
            # TODO(max42): save job count, CPU count, memory limit, block cache size.
            "clique": None
        }

        if self.operation_id is not None:
            self.clique_version = yt.get_operation(self.operation_id, attributes=["spec"]).get("spec", {})\
                .get("description", {}).get("ytserver-clickhouse", {}).get("version", "(unknown_version)")
            logger.info("Clique version is %s", self.clique_version)

        self.salt = hex(random.randint(2**12, 2**16 - 1))[2:]

        self.invocation_key = "{testset}:{clique_version}:{operation_id}:{salt}".format(
            testset=self.testset.key,
            clique_version=self.clique_version,
            operation_id=self.operation_id,
            salt=self.salt)
        logger.info("Invocation key is %s", self.invocation_key)
        self.invocation_path = INVOCATIONS_DIR_PATH + "/" + self.invocation_key

        if not dry_run:
            yt.create("table", self.invocation_path, attributes={"schema": Invocation.SCHEMA})

    def invoke(self, name_regex=None, tag_expression=None, run_count=1, dry_run=False):
        self.testset.fetch_tests()

        self.name_regex = None
        self.tag_expression = None
        self.run_count = run_count

        if name_regex:
            name_regex = re.compile(name_regex)
        if tag_expression:
            known_tags = self.testset.known_tags()
            logger.debug("Known test tags: %s", known_tags)

        self.results = [None] * len(self.testset.tests())

        test_indices_to_run = []

        for i, test_spec in enumerate(self.testset.tests()):
            skip = False
            if name_regex and not name_regex.match(test_spec.name):
                logger.debug("Skipping test %s by regex", test_spec.name)
                skip = True
            if tag_expression and not eval(tag_expression, {tag: (tag in test_spec.tags) for tag in known_tags}):
                logger.debug("Skipping test %s by tag expression", test_spec.name)
                skip = True

            if skip:
                self.results[i] = [
                    Invocation.TestResult(name=test_spec.name, run_index=i, outcome="skipped",
                                          queries=test_spec.queries, reference=test_spec.reference, output=None,
                                          execution_time=None, error=None)
                    for i in range(run_count)
                ]
            else:
                test_indices_to_run.append(i)

        logger.info("Invoking %d tests", len(test_indices_to_run))

        for i in test_indices_to_run:
            test_spec = self.testset.tests()[i]
            logger.info("Invoking test %s%s for %d times", test_spec.name,
                        " " + str(test_spec.tags) if test_spec.tags else "", run_count)

            self.results[i] = [self.invoke_test(test_spec, run_index=i, dry_run=dry_run) for i in range(run_count)]

    def invoke_test(self, test_spec, run_index=0, dry_run=False):
        logger.info("Invoking test %s, run %d", test_spec.name, run_index)
        output = b""

        if dry_run:
            return None

        start_time = time.time()

        try:
            for query in test_spec.queries:
                query = query.format(
                    **{table: "`" + table_path + "`" for table, table_path in self.table_paths.items()})
                logger.debug("Querying '%s'", query)

                current_output = b'\n'.join(chyt.execute(query, self.alias, raw=True))

                # TODO(max42): need of this may mean that references are ill-formed.
                if current_output == b"":
                    continue

                if output == b"":
                    output = current_output
                else:
                    output += b"\n" + current_output
        except yt.YtError as error:
            finish_time = time.time()
            error_file_name = f"{test_spec.name}.{run_index}.{self.salt}.error"
            logger.error("Run: cat %s", error_file_name)

            with open(error_file_name, "w") as error_file:
                error_file.write(str(error))

            return Invocation.TestResult(name=test_spec.name, run_index=run_index, outcome="fail",
                                         queries=test_spec.queries, reference=test_spec.reference, output=None,
                                         execution_time=finish_time - start_time, error=error.simplify())

        finish_time = time.time()

        # TODO(max42): need of this may mean that references are ill-formed.
        output = output.rstrip()

        if test_spec.reference is not None:
            if output == test_spec.reference:
                outcome = "ok"
            else:
                output_file_name = f"{test_spec.name}.{run_index}.{self.salt}.output"
                reference_file_name = f"{test_spec.name}.{run_index}.{self.salt}.reference"

                logger.error("Run: diff ./%s ./%s", output_file_name, reference_file_name)
                outcome = "wrong_output"
                with open(output_file_name, "wb") as output_file:
                    output_file.write(output)
                with open(reference_file_name, "wb") as reference_file_name:
                    reference_file_name.write(test_spec.reference)
        elif test_spec.reference is None:
            outcome = "ok_no_reference"
        logger.info("Test %s, run %d completed, outcome = %s, execution time = %.2f", test_spec.name, run_index,
                    outcome, finish_time - start_time)

        return Invocation.TestResult(name=test_spec.name, run_index=run_index, outcome=outcome,
                                     queries=test_spec.queries, reference=test_spec.reference, output=output,
                                     execution_time=finish_time - start_time, error=None)

    def finish(self, annotation=None, attributes=None, dry_run=False):
        if dry_run:
            return
        yt.write_table(self.invocation_path, [result._asdict() for multi_run_result in self.results
                                              for result in multi_run_result])
        yt.set(self.invocation_path + "/@environment", self.environment)
        yt.set(self.invocation_path + "/@testset", {
            "key": self.testset.key,
            "path": self.testset.path,
        })
        yt.set(self.invocation_path + "/@test_filter", {"name_regex": self.name_regex,
                                                        "tag_expression": self.tag_expression})
        yt.set(self.invocation_path + "/@run_count", self.run_count)

        if annotation:
            yt.set(self.invocation_path + "/@annotation", annotation)
        if attributes:
            for key, value in attributes.items():
                yt.set(self.invocation_path + "/@" + key, value)

        outcome_statistics = collections.Counter()
        for multi_run_result in self.results:
            for result in multi_run_result:
                outcome_statistics[result.outcome] += 1
        logger.info("Outcome statistics: %s", outcome_statistics)
        yt.set(self.invocation_path + "/@outcome_statistics", outcome_statistics)
        self.outcome_statistics = outcome_statistics

        logger.info("Invocation results written to %s", self.invocation_path)


def collect_testsets() -> typing.List[Testset]:
    testsets = []
    for node in yt.list(TESTSET_DIR_PATH, attributes=Testset.ATTRIBUTES):
        testset = Testset(node.attributes)
        testsets.append(testset)
    return testsets


def get_testset(testset_key) -> Testset:
    testsets = [testset for testset in collect_testsets() if testset.key == testset_key]
    if len(testsets) == 0:
        raise yt.YtError("Testset %s not found", testset_key)
    assert len(testsets) == 1
    return testsets[0]


# Pretty table helpers.

def create_pretty_table(columns):
    table = prettytable.PrettyTable(field_names=columns, hrules=prettytable.ALL)
    for attribute in columns:
        table.align[attribute] = "l"
        table.valign[attribute] = "m"
    return table


def format_multiline(cell):
    if isinstance(cell, (dict, yson.YsonMap, list, yson.YsonList)):
        return yson.dumps(cell, yson_format="pretty").decode("ascii")
    return cell


def add_row(table, row):
    table.add_row(list(map(format_multiline, row)))


def list_testsets():
    testsets = collect_testsets()

    table = create_pretty_table(Testset.ATTRIBUTES)
    table.max_width["annotation"] = 50
    for testset in testsets:
        add_row(table, [getattr(testset, attribute) for attribute in Testset.ATTRIBUTES])
    print(table.get_string())


def invoke_testset(testset_key, alias, table_paths=None, name_regex=None, tag_expression=None,
                   dry_run=False, run_count=3, attributes=None, annotation=None):
    testset = get_testset(testset_key)

    table_paths = copy.deepcopy(testset.default_table_paths) if not table_paths \
        else yt.common.update(testset.default_table_paths, table_paths)

    from yt.clickhouse.clickhouse import _resolve_alias

    if dry_run:
        operation_id = None
    else:
        if not alias.startswith("*"):
            raise yt.YtError("Clique alias should start with an asterisk")
        else:
            result = _resolve_alias(alias)
            if result is None:
                raise yt.YtError("Operation with alias {} not found".format(alias))
            if result["state"] != "running":
                raise yt.YtError("Operation with alias {} is not running; actual state = {}"
                                 .format(alias, result["state"]))
            operation_id = result["id"]

    invocation = Invocation(testset, table_paths, alias, operation_id)

    try:
        invocation.prepare(dry_run=dry_run)
        invocation.invoke(name_regex=name_regex, tag_expression=tag_expression, dry_run=dry_run,
                          run_count=run_count)
        invocation.finish(dry_run=dry_run, annotation=annotation, attributes=attributes)
    finally:
        invocation.abort_tx()

    return invocation


def invoke_testset_cli(**kwargs):
    if "attributes" in kwargs:
        if kwargs["attributes"] is not None:
            kwargs["attributes"] = yson.loads(kwargs["attributes"])
        else:
            del kwargs["attributes"]
    result = invoke_testset(**kwargs)
    print(result.invocation_path)


def tag_test(testset_key, test_name, tags):
    testset = get_testset(testset_key)

    testset.fetch_tests()
    testset.tag_test(test_name, tags)
    testset.write()


def list_invocations(testset_key):
    COLUMNS = ["key", "testset_key", "test_filter", "outcome_statistics"]
    table = create_pretty_table(COLUMNS)
    for node in yt.list(INVOCATIONS_DIR_PATH, attributes=COLUMNS):
        row = {attribute: node.attributes.get(attribute) for attribute in COLUMNS}
        row["testset_key"] = node.attributes.get("testset", {}).get("key")
        if testset_key and row["testset_key"] != testset_key:
            pass
        add_row(table, [row[column] for column in COLUMNS])
    print(table.get_string())


def compare_performance(target_invocation_key, baseline_invocation_key=None, testset_key=None, dry_run=False):
    if testset_key is None:
        testset_key = yt.get(INVOCATIONS_DIR_PATH + "/" + target_invocation_key + "/@testset/key")
    if baseline_invocation_key is None:
        baseline_invocation_key = yt.get(BASELINE_INVOCATIONS_PATH + "/" + testset_key)
    logger.info("Target invocation key is %s", target_invocation_key)
    logger.info("Baseline invocation key is %s", baseline_invocation_key)

    if dry_run:
        return

    INVOCATION_ATTRIBUTES = ["testset", "environment", "annotation", "path"]
    target_invocation_attrs = yt.get(INVOCATIONS_DIR_PATH + "/" + target_invocation_key + "/@",
                                 attributes=INVOCATION_ATTRIBUTES)
    testset_key = target_invocation_attrs["testset"]["key"]

    logger.debug("Target testset key is %s", testset_key)

    baseline_invocation_attrs = yt.get(INVOCATIONS_DIR_PATH + "/" + baseline_invocation_key + "/@",
                                       attributes=INVOCATION_ATTRIBUTES)
    if testset_key != baseline_invocation_attrs["testset"]["key"]:
        raise yt.YtError("Invocations correspond to different testsets: {} and {}".format(
            testset_key, baseline_invocation_attrs["testset"]["key"]))

    def aggregate_times(invocation_key):
        rows = yt.read_table(INVOCATIONS_DIR_PATH + "/" + invocation_key)
        test_name_to_times = collections.defaultdict(list)
        for row in rows:
            if row["outcome"] in ("ok", "ok_no_reference"):
                test_name_to_times[row["name"]].append(row["execution_time"])
        return test_name_to_times

    baseline_times = aggregate_times(baseline_invocation_key)
    target_times = aggregate_times(target_invocation_key)
    # TODO(max42): use environment here.
    assert set(baseline_times.keys()) == set(target_times.keys())
    test_names = sorted(target_times.keys())

    print("Comparing two invocations (baseline, target):")
    for invocation_attrs in (baseline_invocation_attrs, target_invocation_attrs):
        print(yson.dumps(invocation_attrs, yson_format="pretty").decode("utf-8"))

    COLUMNS = ["test_name", "baseline", "target", "fraction"]
    table = create_pretty_table(COLUMNS)
    sum_log = 0
    for test_name in test_names:
        baseline_time = min(baseline_times[test_name])
        target_time = min(target_times[test_name])
        fraction = target_time / baseline_time
        formatted_fraction = \
            (colorama.Fore.GREEN if fraction <= 1.0 else colorama.Fore.RED) + \
            "{:.3}".format(fraction) + \
            colorama.Fore.RESET
        add_row(table, [test_name, baseline_time, target_time, formatted_fraction])
        sum_log += math.log(fraction)
    print(table.get_string())
    print("Fraction geometric mean: {:.3}".format(math.exp(sum_log / len(test_names))))


# Logging helpers.


def setup_logging(args):
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s\t%(levelname).1s\t%(module)s:%(lineno)d\t%(message)s")
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel([logging.INFO, logging.DEBUG][min(args.verbose, 1)])
    stderr_handler.setFormatter(formatter)
    file_handler = logging.FileHandler("tester.debug.log", "a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)
    logger.addHandler(file_handler)
    logger.info("Logging started, invocation args = %s", args)


def main():
    colorama.init()

    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='count', default=0, help="Log stuff, more v's for more logging")

    subparsers = parser.add_subparsers()

    list_testsets_parser = subparsers.add_parser("list-testsets", help=f"List testsets available in {TESTSET_DIR_PATH}")
    list_testsets_parser.set_defaults(func=list_testsets)

    invoke_testset_parser = subparsers.add_parser("invoke-testset", help="Invoke testset on given clique")
    invoke_testset_parser.add_argument("--testset", help="Testset key", required=True, dest="testset_key")
    invoke_testset_parser.add_argument("--alias", help="Clique alias (with asterisk)", required=True)
    invoke_testset_parser.add_argument("--table-paths", help="Dict with table substitutions to override "
                                                             "the default ones")
    invoke_testset_parser.add_argument("--name-regex", help="Regex for filtering test names")
    invoke_testset_parser.add_argument("--tag-expression", help="pytest-style expression for tags; for example, "
                                                                "'skip and not xfail'; defaults to 'not skip and not "
                                                                "xfail'",
                                       default="not skip and not xfail")
    invoke_testset_parser.add_argument("--run-count", help="Test run count; may be useful for performance tests to "
                                                           "eliminate time dispersion or to consider block cache "
                                                           "effect", type=int, default=1)
    invoke_testset_parser.add_argument("--dry-run", help="Do not actually run tests and do not write results to table",
                                       action="store_true")
    invoke_testset_parser.add_argument("--annotation", help="Arbitrary message describing this invocation")
    invoke_testset_parser.add_argument("--attributes", help="Arbitrary YSON attributes describing this invocation")
    invoke_testset_parser.set_defaults(func=invoke_testset_cli)

    tag_test_parser = subparsers.add_parser("tag-test", help="Tag test from testset; tags will be overridden with "
                                                             "given tag list")
    tag_test_parser.add_argument("--testset-key", help="Testset key", required=True)
    tag_test_parser.add_argument("--test-name", help="Test name", required=True)
    tag_test_parser.add_argument("--tags", help="Tags; may be empty", nargs="*", required=True)
    tag_test_parser.set_defaults(func=tag_test)

    list_invocations_parser = subparsers.add_parser("list-invocations", help="List previous invocations")
    list_invocations_parser.add_argument("--testset-key", help="Testset key")
    list_invocations_parser.set_defaults(func=list_invocations)

    compare_performance_parser = subparsers.add_parser("compare-performance",
                                                       help="Compare performance of two invocations on same testset")
    compare_performance_parser.add_argument("--target-invocation-key", required=True,
                                            help="target invocation key")
    compare_performance_parser.add_argument("--baseline-invocation-key",
                                            help="baseline invocation key; may be omitted, in which case assumed to"
                                                 "be the invocation fixed in 'baseline_invocations' document")
    compare_performance_parser.set_defaults(func=compare_performance)

    args = parser.parse_args()

    setup_logging(args)

    func_args = dict(vars(args))

    if "verbose" in func_args:
        del func_args["verbose"]
    del func_args["func"]

    args.func(**func_args)


if __name__ == "__main__":
    main()
