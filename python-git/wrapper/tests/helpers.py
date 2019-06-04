from __future__ import print_function

from yt.packages.six import iteritems, integer_types, text_type, binary_type, b
from yt.packages.six.moves import map as imap

from yt.test_helpers import wait
from yt.test_helpers.job_events import JobEvents

import yt.yson as yson
import yt.subprocess_wrapper as subprocess

import yt.wrapper as yt

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

import collections
import glob
import os
import shutil
import sys
import stat
import tempfile
from contextlib import contextmanager
from copy import deepcopy

TEST_DIR = "//home/wrapper_tests"

TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))
PYTHONPATH = os.path.abspath(os.path.join(TESTS_LOCATION, "../../../"))
TESTS_SANDBOX = os.environ.get("TESTS_SANDBOX", TESTS_LOCATION + ".sandbox")
ENABLE_JOB_CONTROL = bool(int(os.environ.get("TESTS_JOB_CONTROL", False)))

def get_tests_location():
    if yatest_common is not None:
        return yatest_common.source_path("yt/python/yt/wrapper/tests")
    else:
        return TESTS_LOCATION

def get_tests_sandbox():
    path = os.environ.get("TESTS_SANDBOX")
    tmpfs_path = get_tmpfs_path()
    if path is None:
        if yatest_common is not None:
            if tmpfs_path is None:
                path = os.path.join(yatest_common.output_path(), "sandbox")
            else:
                path = os.path.join(tmpfs_path, "sandbox")
        else:
            path = TESTS_SANDBOX
    if not os.path.exists(path):
        os.mkdir(path)
    return path

def get_test_dir_path():
    return os.path.join(get_tests_location(), "files")

def get_test_file_path(name):
    return os.path.join(get_test_dir_path(), name)

def get_tmpfs_path():
    if yatest_common is not None and yatest_common.get_param("ram_drive_path") is not None:
        path = yatest_common.get_param("ram_drive_path")
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    return None

def get_port_locks_path():
    path = get_tmpfs_path()
    if path is None:
         path = get_tests_sandbox()
    return os.path.join(path, "ports")

def get_python():
    if yatest_common is None:
        return sys.executable
    else:
        return yatest_common.binary_path("yt/python/yt/wrapper/tests/yt_python/yt-python")

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

@contextmanager
def set_config_options(options_dict):
    old_values = {}
    for key in options_dict:
        old_values[key] = yt.config._get(key)
    try:
        for key, value in iteritems(options_dict):
            yt.config._set(key, value)
        yield
    finally:
        for key, value in iteritems(old_values):
            yt.config._set(key, value)

# Check equality of records
def check(rowsA, rowsB, ordered=True):
    def prepare(rows):
        def fix_unicode(obj):
            if isinstance(obj, text_type):
                return str(obj)
            return obj
        def process_row(row):
            if isinstance(row, dict):
                return dict([(fix_unicode(key), fix_unicode(value)) for key, value in iteritems(row)])
            return row

        rows = list(imap(process_row, rows))
        if not ordered:
            rows = tuple(sorted(imap(lambda obj: tuple(sorted(iteritems(obj))), rows)))

        return rows

    lhs, rhs = prepare(rowsA), prepare(rowsB)
    assert lhs == rhs

def _filter_simple_types(obj):
    if isinstance(obj, integer_types) or \
            isinstance(obj, float) or \
            obj is None or \
            isinstance(obj, yson.YsonType) or \
            isinstance(obj, (binary_type, text_type)):
        return obj
    elif isinstance(obj, list):
        return [_filter_simple_types(item) for item in obj]
    elif isinstance(obj, collections.Mapping):
        return dict([(key, _filter_simple_types(value)) for key, value in iteritems(obj)])
    return None

def get_environment_for_binary_test(yt_env):
    if yatest_common is None:
        python_binary = sys.executable
    else:
        python_binary = get_python()

    binaries_dir = os.path.join(os.path.dirname(get_tests_location()), "bin")

    env = {
        "PYTHON_BINARY": python_binary,
        "YT_USE_TOKEN": "0",
        "YT_VERSION": yt.config["api_version"],
        "YT_PRINT_BACKTRACE": "1",
        "YT_SCRIPT_PATH": os.path.join(binaries_dir, "yt"),
        "MAPREDUCE_YT_SCRIPT_PATH": os.path.join(binaries_dir, "mapreduce-yt")
    }
    if yatest_common is None:
        env["PYTHONPATH"] = os.environ["PYTHONPATH"]

    config = deepcopy(_filter_simple_types(yt.config.config))

    if config["backend"] == "native":
        _, filename = tempfile.mkstemp(dir=get_tests_sandbox(), prefix="binary_test_driver_config")
        with open(filename, "wb") as f:
            yson.dump({"driver": config["driver_config"], "logging": yt_env.env.driver_logging_config}, f)

        config["driver_config"] = None
        config["driver_config_path"] = filename

    env["YT_CONFIG_PATCHES"] = yson._dumps_to_native_str(config)
    return env

def build_python_egg(egg_contents_dir, temp_dir=None):
    dir_ = tempfile.mkdtemp(dir=temp_dir)

    for obj in os.listdir(egg_contents_dir):
        src = os.path.join(egg_contents_dir, obj)
        dst = os.path.join(dir_, obj)
        if os.path.isdir(src):
            shutil.copytree(src, dst)
        else:  # file
            shutil.copy2(src, dst)

    _, egg_filename = tempfile.mkstemp(dir=temp_dir, suffix=".egg")
    try:
        subprocess.check_call([get_python(), "setup.py", "bdist_egg"], cwd=dir_)

        eggs = glob.glob(os.path.join(dir_, "dist", "*.egg"))
        assert len(eggs) == 1

        shutil.copy2(eggs[0], egg_filename)
        return egg_filename
    finally:
        shutil.rmtree(dir_, ignore_errors=True)

def dumps_yt_config():
    config = _filter_simple_types(yt.config.config)
    return yson._dumps_to_native_str(config)

def run_python_script_with_check(yt_env, script):
    dir_ = yt_env.env.path

    with tempfile.NamedTemporaryFile(mode="w", dir=dir_, suffix=".py", delete=False) as f:
        f.write(script)
        f.close()

        proc = subprocess.Popen([sys.executable, f.name], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        out, err = proc.communicate(b(dumps_yt_config()))
        assert proc.returncode == 0, err

        return out, err

def sync_create_cell():
    tablet_id = yt.create("tablet_cell", attributes={"size": 1})
    wait(lambda : yt.get("//sys/tablet_cells/{0}/@health".format(tablet_id)) == "good")

def wait_record_in_job_archive(operation_id, job_id):
    operation_id_hash_pair = yt.common.uuid_hash_pair(operation_id)
    job_id_hash_pair = yt.common.uuid_hash_pair(job_id)
    # Jobs
    key = {}
    key["operation_id_hi"], key["operation_id_lo"] = operation_id_hash_pair.hi, operation_id_hash_pair.lo
    key["job_id_hi"], key["job_id_lo"] = job_id_hash_pair.hi, job_id_hash_pair.lo
    wait(lambda: any(yt.lookup_rows("//sys/operations_archive/jobs", [key], column_names=["operation_id_hi"])))
    # Job specs
    key = {}
    key["job_id_hi"], key["job_id_lo"] = job_id_hash_pair.hi, job_id_hash_pair.lo
    wait(lambda: any(yt.lookup_rows("//sys/operations_archive/job_specs", [key], column_names=["spec_version"])))

def get_operation_path(operation_id):
    return "//sys/operations/{:02x}/{}".format(int(operation_id.split("-")[-1], 16) % 256, operation_id)

def create_job_events():
    tmpdir = tempfile.mkdtemp(prefix="job_events", dir=get_tests_sandbox())
    os.chmod(tmpdir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    return JobEvents(tmpdir)

