import yt.wrapper as yt

import os
from contextlib import contextmanager

TEST_DIR = "//home/wrapper_tests"

TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))
PYTHONPATH = os.path.abspath(os.path.join(TESTS_LOCATION, "../../../"))
TESTS_SANDBOX = os.environ.get("TESTS_SANDBOX", TESTS_LOCATION + ".sandbox")
ENABLE_JOB_CONTROL = bool(int(os.environ.get("TESTS_JOB_CONTROL", False)))

def get_test_file_path(name):
    return os.path.join(TESTS_LOCATION, "files", name)

@contextmanager
def set_config_option(name, value, final_action=None):
    old_value = yt.config._get(name)
    try:
        yt.config._set(name, value)
        yield
    finally:
        if final_action is not None:
            final_action()
        yt.config._set(name, old_value)

# Check equality of records in dsv format
def check(rowsA, rowsB, ordered=True):
    def prepare(rows):
        def fix_unicode(obj):
            if isinstance(obj, unicode):
                #print >>sys.stderr, obj, str(obj)
                return str(obj)
            return obj
        def process_row(row):
            if isinstance(row, dict):
                return dict([(fix_unicode(key), fix_unicode(value)) for key, value in row.iteritems()])
            return row

        rows = map(process_row, rows)
        if not ordered:
            rows = tuple(sorted(map(lambda obj: tuple(sorted(obj.items())), rows)))

        return rows

    lhs, rhs = prepare(rowsA), prepare(rowsB)
    assert lhs == rhs

def get_environment_for_binary_test():
    env = {
        "PYTHONPATH": os.environ["PYTHONPATH"],
        "YT_USE_TOKEN": "0",
        "YT_VERSION": yt.config["api_version"]
    }
    if yt.config["proxy"]["url"] is not None:
        env["YT_PROXY"] = yt.config["proxy"]["url"]
    if yt.config["driver_config_path"] is not None:
        env["YT_DRIVER_CONFIG_PATH"] = yt.config["driver_config_path"]
    return env

