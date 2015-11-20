import yt.wrapper as yt

import os
import string

TEST_DIR = "//home/wrapper_tests"

TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))

def get_test_file_path(name):
    return os.path.join(TESTS_LOCATION, "files", name)

# Check equality of records in dsv format
def check(recordsA, recordsB):
    def prepare(records):
        return map(yt.loads_row, sorted(list(records)))
    assert prepare(recordsA) == prepare(recordsB)

def get_temp_dsv_records():
    columns = (string.digits, reversed(string.ascii_lowercase[:10]), string.ascii_uppercase)
    def dumps_row(row):
        return "x={0}\ty={1}\tz={2}\n".format(*row)
    return map(dumps_row, zip(*columns))

def get_environment_for_binary_test():
    env = {"PYTHONPATH": os.environ["PYTHONPATH"],
          "YT_USE_TOKEN": "0",
          "YT_VERSION": yt.config["api_version"]}
    if yt.config["proxy"]["url"] is not None:
        env["YT_PROXY"] = yt.config["proxy"]["url"]
    if yt.config["driver_config_path"] is not None:
        env["YT_DRIVER_CONFIG_PATH"] = yt.config["driver_config_path"]
    return env

